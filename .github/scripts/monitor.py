import requests
import hashlib
import os
import sys
from bs4 import BeautifulSoup

def check_website(site_name, website_url):
    alerts = []
    
    # --- UPDATED DIRECTORY LOGIC ---
    # Ensure the folder exists (e.g., /slavonski_brod/)
    os.makedirs(site_name, exist_ok=True)
    # Put the hash file inside that folder
    hash_file = os.path.join(site_name, "hash.txt")
    # -------------------------------

    try:
        response = requests.get(website_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        if soup.body:
            body_content = soup.body.get_text(separator=' ', strip=True)
        else:
            body_content = response.text
            
        current_hash = hashlib.md5(body_content.encode('utf-8')).hexdigest()

        previous_hash = ""
        if os.path.exists(hash_file):
            with open(hash_file, 'r') as f:
                previous_hash = f.read().strip()

        if current_hash != previous_hash and previous_hash != "":
            alerts.append(f"🌐 **Website Change Detected:** The readable content at {website_url} has been updated!")
        
        with open(hash_file, 'w') as f:
            f.write(current_hash)
            
    except Exception as e:
        alerts.append(f"⚠️ Error checking {site_name} ({website_url}): {e}")

    if alerts:
        print(f"Alerts found for {site_name}! Writing to file...")
        with open("alerts.txt", "a") as f:
            f.write("\n\n".join(alerts) + "\n\n")
    else:
        print(f"{site_name} body is unchanged. No alerts today.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Error: Missing arguments. Usage: python monitor.py <site_name> <url>")
        sys.exit(1)
        
    site_name = sys.argv[1]
    website_url = sys.argv[2]
    check_website(site_name, website_url)
