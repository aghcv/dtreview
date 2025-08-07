import csv
import requests
from Bio import Entrez, Medline
import xml.etree.ElementTree as ET
from collections import Counter
import matplotlib.pyplot as plt
from pathlib import Path



# ======== CONFIG =========
Entrez.email = "your_email@example.com"  # Required by NCBI
OOS_DIR = Path("oos")  # Out-of-source directory for outputs
OOS_DIR.mkdir(parents=True, exist_ok=True)

# Create/append .gitignore to exclude oos/ from commits
gitignore_path = Path(".gitignore")
if gitignore_path.exists():
    existing_lines = gitignore_path.read_text().splitlines()
else:
    existing_lines = []

if "oos/" not in existing_lines:
    with open(gitignore_path, "a", encoding="utf-8") as gitignore:
        gitignore.write("\noos/\n")

# ======== SEARCH CONFIG =========
QUERY = '("digital twin" OR "virtual patient" OR "in silico patient" OR "surrogate model" OR "physics-informed neural network") AND (multiscale OR multiphysics OR hybrid modeling OR "precision medicine" OR workflow OR "model validation" OR "uncertainty quantification")'
MAX_RESULTS = 100000
results = []

# ======== PUBMED =========
def search_pubmed():
    handle = Entrez.esearch(db="pubmed", term=QUERY, retmax=MAX_RESULTS)
    record = Entrez.read(handle)
    handle.close()
    ids = record["IdList"]
    handle = Entrez.efetch(db="pubmed", id=",".join(ids), rettype="medline", retmode="text")
    records = list(Medline.parse(handle))
    handle.close()
    for rec in records:
        results.append({
            "Source": "PubMed",
            "Title": rec.get("TI", "").strip(),
            "Authors": "; ".join(rec.get("AU", [])),
            "Year": rec.get("DP", "").split(" ")[0],
            "Abstract": rec.get("AB", ""),
            "DOI": rec.get("LID", ""),
            "URL": f"https://pubmed.ncbi.nlm.nih.gov/{rec.get('PMID','')}/"
        })

# ======== ARXIV =========
def search_arxiv():
    base_url = "http://export.arxiv.org/api/query"
    start = 0
    batch_size = 200  # arXiv allows up to 200 per query
    retrieved = 0
    while retrieved < MAX_RESULTS:
        url = f"{base_url}?search_query=all:{QUERY}&start={start}&max_results={batch_size}"
        r = requests.get(url)
        feed = ET.fromstring(r.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = feed.findall('atom:entry', ns)
        if not entries:
            break
        for entry in entries:
            results.append({
                "Source": "arXiv",
                "Title": entry.find('atom:title', ns).text.strip(),
                "Authors": "; ".join([a.find('atom:name', ns).text for a in entry.findall('atom:author', ns)]),
                "Year": entry.find('atom:published', ns).text.split("-")[0],
                "Abstract": entry.find('atom:summary', ns).text,
                "DOI": "",
                "URL": entry.find('atom:id', ns).text
            })
        retrieved += len(entries)
        start += batch_size


# ======== BIORXIV =========
# to be added later

# ======== CROSSREF =========
def search_crossref():
    url = "https://api.crossref.org/works"
    params = {
        "query": QUERY,
        "rows": 50
    }
    r = requests.get(url, params=params)
    if r.status_code != 200:
        print(f"[CrossRef] Failed request (status {r.status_code})")
        return
    
    data = r.json()
    for item in data.get("message", {}).get("items", []):
        title = item.get("title", [""])[0]
        authors = []
        for a in item.get("author", []):
            name_parts = []
            if "given" in a:
                name_parts.append(a["given"])
            if "family" in a:
                name_parts.append(a["family"])
            authors.append(" ".join(name_parts))
        
        results.append({
            "Source": "CrossRef",
            "Title": title.strip(),
            "Authors": "; ".join(authors),
            "Year": str(item.get("issued", {}).get("date-parts", [[None]])[0][0]),
            "Abstract": "",  # CrossRef rarely has abstracts
            "DOI": item.get("DOI", ""),
            "URL": item.get("URL", "")
        })

# ======== EUROPE PMC =========
def search_europe_pmc():
    base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    page_size = 100  # Europe PMC max per request
    retrieved = 0
    page = 1

    while retrieved < MAX_RESULTS:
        params = {
            "query": QUERY,
            "format": "json",
            "pageSize": page_size,
            "page": page
        }
        r = requests.get(base_url, params=params)
        if r.status_code != 200:
            print(f"[Europe PMC] Failed request (status {r.status_code})")
            break
        data = r.json()
        hit_list = data.get("resultList", {}).get("result", [])
        if not hit_list:
            break
        for hit in hit_list:
            results.append({
                "Source": "Europe PMC",
                "Title": hit.get("title", "").strip(),
                "Authors": "; ".join([a.get("fullName", "") for a in hit.get("authorList", {}).get("author", [])]),
                "Year": str(hit.get("pubYear", "")),
                "Abstract": hit.get("abstractText", ""),
                "DOI": hit.get("doi", ""),
                "URL": f"https://europepmc.org/article/{hit.get('source', '')}/{hit.get('id', '')}"
            })
        retrieved += len(hit_list)
        page += 1

# ======== RUN SEARCHES =========
search_pubmed()
search_arxiv()
search_crossref()
search_europe_pmc()

# ======== SUMMARY BEFORE DEDUP =========
source_counts_before = Counter([r["Source"] for r in results])
print("\n=== Results per Source (Before Deduplication) ===")
for src, count in source_counts_before.items():
    print(f"{src}: {count}")
print(f"Total: {len(results)}")

# ======== REMOVE DUPLICATES (based on Title + Year) =========
seen = set()
unique_results = []
for r in results:
    key = (r["Title"].lower(), r["Year"])
    if key not in seen:
        seen.add(key)
        unique_results.append(r)

# ======== SUMMARY AFTER DEDUP =========
source_counts_after = Counter([r["Source"] for r in unique_results])
print("\n=== Results per Source (After Deduplication) ===")
for src, count in source_counts_after.items():
    print(f"{src}: {count}")
print(f"Total: {len(unique_results)}")

# ======== SAVE TO CSV =========
csv_path = OOS_DIR / "combined_results.csv"
with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Source", "Title", "Authors", "Year", "Abstract", "DOI", "URL"])
    writer.writeheader()
    for row in unique_results:
        writer.writerow(row)

print(f"\nSaved {len(unique_results)} unique records to {csv_path}")

# ======== VISUALIZE RESULTS =========
fig, ax = plt.subplots(1, 2, figsize=(10, 5))

# Before deduplication
ax[0].bar(source_counts_before.keys(), source_counts_before.values(), color='skyblue')
ax[0].set_title("Before Deduplication")
ax[0].set_ylabel("Record Count")
ax[0].tick_params(axis='x', rotation=30)

# After deduplication
ax[1].bar(source_counts_after.keys(), source_counts_after.values(), color='lightgreen')
ax[1].set_title("After Deduplication")
ax[1].tick_params(axis='x', rotation=30)

plt.tight_layout()
plot_path = OOS_DIR / "results_summary.png"
plt.savefig(plot_path, dpi=300)
plt.show()
