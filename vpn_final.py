import time
import os
import logging
import requests
import base64
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import re
import random
import subprocess

# ------------------------
# EMAIL CONFIGURATION
# ------------------------
POSTMARK_API_TOKEN = "ad610f28-69e3-4179-93ce-66ae03e118e5"
FROM_EMAIL = "itsupport@exchange-data.in"
TO_EMAILS = [""]
CC_EMAILS = [""]

dwnld_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.95 Safari/537.36',
    'Connection': 'keep-alive'
}

headers = [{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.7339.207 Safari/537.36'}]

# ------------------------
# VPN CONFIGURATION
# ------------------------
VPN_CONNECT_SCRIPT = "/opt/apps/ExpressVPN/vpn-connect-usny.sh"
VPN_DISCONNECT_SCRIPT = "/opt/apps/ExpressVPN/vpn-disconnect-with-DNS-Reset.sh"

def connect_vpn():
    try:
        logging.info(f"Trying VPN: {VPN_CONNECT_SCRIPT}")
        subprocess.run([VPN_CONNECT_SCRIPT], check=True)
        logging.info("VPN connected successfully.")
        time.sleep(30)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to connect VPN: {e}")
        return False


def disconnect_vpn():
    try:
        subprocess.run([VPN_DISCONNECT_SCRIPT], check=True)
        logging.info("VPN disconnected.")
        time.sleep(30)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error disconnecting VPN: {e}")

# ------------------------
# EMAIL FUNCTION
# ------------------------
def send_email(subject, body, attachment_path=None):
    try:
        email_data = {
            "From": FROM_EMAIL,
            "To": ",".join(TO_EMAILS),
            "Cc": ",".join(CC_EMAILS) if CC_EMAILS else None,
            "Subject": subject,
            "TextBody": body,
        }

        email_data = {k: v for k, v in email_data.items() if v}

        if attachment_path:
            if isinstance(attachment_path, list):
                attachments = []
                for path in attachment_path:
                    if os.path.exists(path):
                        with open(path, "rb") as f:
                            encoded = base64.b64encode(f.read()).decode()
                        attachments.append({
                            "Name": os.path.basename(path),
                            "Content": encoded,
                            "ContentType": "application/octet-stream"
                        })
                email_data["Attachments"] = attachments

        response = requests.post(
            "https://api.postmarkapp.com/email",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Postmark-Server-Token": POSTMARK_API_TOKEN,
            },
            json=email_data,
        )

        if response.status_code == 200:
            print(" Email sent successfully.")
            logging.info("Email sent successfully.")
        else:
            print(f" Email failed: {response.status_code}")
            print(response.text)
            logging.error(response.text)

    except Exception as e:
        logging.error(f"Email sending failed: {str(e)}")
        print(" Email exception:", str(e))


# ------------------------
# PATHS
# ------------------------
base_path_input = r"C:\Users\pechazhagan.r\Documents\MSRB_VB_2\Formate_file\Security_1"
base_path_output = r"C:\Users\pechazhagan.r\Documents\MSRB_VB_2\Formate_file\Security_1\output"

input_path = os.path.join(base_path_input, "security_1_add.xlsx")
output_path = os.path.join(base_path_output, "security_1_add_output.xlsx")

log_folder = os.path.join(base_path_input, "logs")
os.makedirs(log_folder, exist_ok=True)

log_file = os.path.join(
    log_folder,
    f"scraping_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ------------------------
# CONNECT VPN BEFORE START
# ------------------------
connect_vpn()

# ------------------------
# LOAD INPUT
# ------------------------
df = pd.read_excel(input_path)
df["security_details_url"] = df["security_details_url"].astype(str)

# ------------------------
# REQUIRED OUTPUT COLUMNS
# ------------------------
required_columns = [
    "Long Description","Short Description","State","Coupon","Interest Rate",
    "Maturity Date","Dated Date","Price","Yield",
    "Principal Amount at Issuance","Reset Period","Maximum Rate","Minimum Rate",
    "Time of Formal Award","Time of First Execution","Closing Date","FY EndDate",
    "Minimum Denomination","Notification Period","Remarketing Agent",
    "Liquidity Facility","Provider Identity","Expiration","Tender Agents"
]

for col in required_columns:
    if col not in df.columns:
        df[col] = ""

column_mapping = {
    "Coupon": "Coupon",
    "Interest Rate": "Interest Rate",
    "Maturity Date": "Maturity Date",
    "Dated Date": "Dated Date",
    "Principal Amount at Issuance": "Principal Amount at Issuance",
    "Reset Period": "Reset Period",
    "Maximum Rate": "Maximum Rate",
    "Minimum Rate": "Minimum Rate",
    "Minimum Denomination": "Minimum Denomination",
    "Notification Period": "Notification Period",
    "Remarketing Agent": "Remarketing Agent",
    "Liquidity Facility": "Liquidity Facility",
    "Provider Identity": "Provider Identity",
    "Expiration": "Expiration",
    "Tender Agents": "Tender Agents",
    "Time of Formal Award": "Time of Formal Award",
    "Time of First Execution": "Time of First Execution",
    "Closing Date": "Closing Date",
    "Fiscal Year End Date": "FY EndDate"
}

# ------------------------
# CHROME SETUP
# ------------------------
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

random_header = random.choice(headers)
chrome_options.add_argument(f"user-agent={random_header['User-Agent']}")

chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 20)

disclaimer_handled = False
script_failed = False
total_rows = len(df)
error_url=[]

# ------------------------
# MAIN LOOP
# ------------------------
for index, row in df.iterrows():

    url = row["security_details_url"].strip()

    print(f"Processing ({index+1}/{total_rows}): {url}")
    logging.info(f"Processing ({index+1}/{total_rows}): {url}")

    success = False

    for i in range(5):

        try:
            driver.get(url)

            if not disclaimer_handled:
                try:
                    accept = WebDriverWait(driver, 8).until(
                        EC.element_to_be_clickable(
                            (By.ID,"ctl00_mainContentArea_disclaimerContent_yesButton")
                        )
                    )
                    driver.execute_script("arguments[0].click();", accept)
                    time.sleep(1)
                except:
                    pass

                disclaimer_handled = True

            try:
                no_record_element = driver.find_element(
                    By.XPATH,
                    "//span[contains(text(),'no records exist for this CUSIP')]"
                )

                if no_record_element:
                    df.at[index,"Long Description"]="no data found"
                    df.at[index,"Short Description"]="no data found"
                    success=True
                    break

            except:
                pass

            try:
                show_more = driver.find_element(By.ID,"lnkMoreInfo")

                if "+Show More" in show_more.text:
                    driver.execute_script("arguments[0].click();",show_more)
                    time.sleep(1)
            except:
                pass

            wait.until(EC.presence_of_element_located((By.CLASS_NAME,"card-body")))

            try:
                df.at[index,"Long Description"] = driver.find_element(
                    By.CSS_SELECTOR,
                    "a[href*='/IssueView/Details/'] h3 span"
                ).text.strip()
            except:
                df.at[index,"Long Description"]=""

            try:
                short_desc = driver.find_element(
                    By.CSS_SELECTOR,"h5 span"
                ).text.strip()

                df.at[index,"Short Description"]=short_desc.rstrip("*").strip()

            except:
                df.at[index,"Short Description"]=""

            items = driver.find_elements(
                By.CSS_SELECTOR,
                "div.card-body ul.info-focus li, div.card-body ul.info-focus-2 li"
            )

            for item in items:

                try:

                    try:
                        label_element=item.find_element(By.CSS_SELECTOR,"span.label")
                    except:
                        label_element=item.find_element(By.CSS_SELECTOR,"label.label")

                    label=label_element.text.replace(":","").strip()
                    value=item.find_element(By.CSS_SELECTOR,"span.float-right").text.strip()

                    if label=="Initial Offering Price/Yield":

                        parts=value.split("/")

                        price=parts[0].strip()
                        yield_value=parts[1].strip() if len(parts)>1 else ""

                        price=price.replace(",","").strip()
                        yield_value=yield_value.replace("%","%").strip()

                        df.at[index,"Price"]=price
                        df.at[index,"Yield"]=yield_value

                    elif label in column_mapping:

                        column_name=column_mapping[label]

                        if column_name=="Coupon":
                            clean_value=value.replace("%","").replace("\u00A0","").strip()

                            try:
                                clean_value=int(clean_value)
                            except:
                                pass

                        if column_name=="Principal Amount at Issuance":
                            clean_value=value.replace(",","").replace("$","").strip()

                        else:
                            clean_value=value.strip()

                        df.at[index,column_name]=clean_value

                except:
                    continue

            success=True
            break

        except Exception as e:

            logging.error(str(e))
            logging.info(f"error occured retrying:{url}")

            print("retrying ",url)
            time.sleep(120)

    if not success:
        print(f" Failed after 5 retries: {url}")
        logging.error(f"Failed after 5 retries: {url}")
        error_url.append(url)

    if (index+1)%100==0:

        df.to_excel(output_path,index=False)

        print(f"Auto-saved at row {index+1}")
        logging.info(f"Auto-saved at row {index+1}")

    time.sleep(2)

# ------------------------
# FINAL CLEANING
# ------------------------
df["Coupon"]=df["Coupon"].astype(str).str.replace("%","").str.replace("\u00A0","").str.strip()
df["Coupon"]=pd.to_numeric(df["Coupon"],errors='coerce')

def extract_state(long_desc,short_desc):

    long_desc=str(long_desc).strip() if pd.notna(long_desc) else ""
    short_desc=str(short_desc).strip() if pd.notna(short_desc) else ""

    pattern=r"\(([A-Z]{2})\)\*?$"

    if long_desc:
        match=re.search(pattern,long_desc)
        if match:
            return match.group(1)

    if short_desc:
        match=re.search(pattern,short_desc)
        if match:
            return match.group(1)

    return ""

df["State"]=df.apply(lambda x: extract_state(x["Long Description"],x["Short Description"]),axis=1)

df.to_excel(output_path,index=False)

driver.quit()

print(" Final Save Completed.")

# ------------------------
# EMAIL
# ------------------------
email_subject="US Muni Price/Yield Extraction Completed_MSRB_US"
email_body="Scraping completed. Please find attached files."

if error_url:
    script_failed=True

if script_failed:
    send_email(email_subject+" (With Errors)",email_body,[])
else:
    send_email(email_subject+" (Success)",email_body,[output_path,log_file])

# ------------------------
# DISCONNECT VPN
# ------------------------
disconnect_vpn()

print(" Finished Successfully.")