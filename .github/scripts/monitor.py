import requests
import os
import sys
import difflib
from bs4 import BeautifulSoup

def check_website(site_name, website_url):
    alerts = []
    
    # Ensure the folder exists
    os.makedirs(site_name, exist_ok=True)
    
    content_file = os.path.join(site_name, "content.txt")
    html_file = os.path.join(site_name, "page.html") # <-- NEW: Define the HTML file path

    try:
        response = requests.get(website_url)
        response.raise_for_status()

        # --- NEW: Save the raw HTML archive immediately ---
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        # --------------------------------------------------
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        if soup.body:
            body_content = soup.body.get_text(separator='\n', strip=True)
            links = [a.get('href') for a in soup.body.find_all('a') if a.get('href')]
            images = [img.get('src') for img in soup.body.find_all('img') if img.get('src')]
            iframes = [iframe.get('src') for iframe in soup.body.find_all('iframe') if iframe.get('src')]
            
            tracked_content = "--- VISIBLE TEXT ---\n" + body_content + "\n"
            if links:
                tracked_content += "\n--- LINKS ---\n" + "\n".join(links) + "\n"
            if images:
                tracked_content += "\n--- IMAGES ---\n" + "\n".join(images) + "\n"
            if iframes:
                tracked_content += "\n--- IFRAMES ---\n" + "\n".join(iframes) + "\n"
        else:
            tracked_content = response.text

        previous_content = ""
        if os.path.exists(content_file):
            with open(content_file, 'r', encoding='utf-8') as f:
                previous_content = f.read()

        tracked_lines = tracked_content.splitlines()
        previous_lines = previous_content.splitlines()

        # Check if the actual lines of text are different
        if tracked_lines != previous_lines and len(previous_lines) > 0:
            diff = list(difflib.unified_diff(
                previous_lines,
                tracked_lines,
                fromfile='Yesterday',
                tofile='Today',
                lineterm=''
            ))
            
            # Absolute safety catch: Only send an alert if the diff is NOT empty
            if diff:
                diff_text = "\n".join(diff[:50])
                if len(diff) > 50:
                    diff_text += "\n\n... (diff truncated because it was too large)"

                alert_msg = f"🌐 **Website Change Detected:** The readable content at {website_url} has been updated!\n\n**What changed:**\n```diff\n{diff_text}\n```"
                alerts.append(alert_msg)
        
        # Save the readable content for tomorrow's diff comparison
        with open(content_file, 'w', encoding='utf-8') as f:
            f.write(tracked_content)
            
    except Exception as e:
        alerts.append(f"⚠️ Error checking {site_name} ({website_url}): {e}")

    if alerts:
        print(f"Alerts found for {site_name}! Writing to file...")
        with open("alerts.txt", "a", encoding='utf-8') as f:
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
