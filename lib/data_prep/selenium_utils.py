import os
import time
from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
from dotenv import load_dotenv

load_dotenv()

def download_letterboxd_zip(hide_actions=True):
    try:
        chromedriver_autoinstaller.install()
        # chrome_options = Options()
        chrome_options = webdriver.ChromeOptions()
        if hide_actions: chrome_options.add_argument("--headless")
        download_dir =  os.getenv('PROJECT_PATH')+'/db/raw_exports/'
        download_dir = download_dir.replace('/', '\\')
        prefs = {"download.default_directory" : download_dir, "directory_upgrade": True}
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(os.getenv('LETTERBOXD_SETTINGS_PAGE'))
        driver.implicitly_wait(10)
#         accept_cookies = driver.find_element(By.CLASS_NAME, 'fc-button-label')
#         accept_cookies.click()
        username = driver.find_element(By.XPATH, '//*[@id="signin-username"]')
        username.send_keys(os.getenv('LETTERBOXD_USER'))
        password = driver.find_element(By.XPATH, '//*[@id="signin-password"]')
        password.send_keys(os.getenv('LETTERBOXD_PASSWORD'))
        signin_button = driver.find_element(By.XPATH, '//*[@id="signin-form"]/fieldset/div[4]/div/input')
        signin_button.click()
        time.sleep(5)
        driver.execute_script('window.open("{}","_blank");'.format(os.getenv('LETTERBOXD_EXPORT_LINK')))
        time.sleep(10)
        print('Download Successful')
    except:
        print('Download Failed')

def return_logged_in_page_source(page_url, hide_actions=True):
    try:
        chromedriver_autoinstaller.install()
        # chrome_options = Options()
        chrome_options = webdriver.ChromeOptions()
        if hide_actions: chrome_options.add_argument("--headless")
        download_dir =  os.getenv('PROJECT_PATH')+'/db/raw_exports/'
        download_dir = download_dir.replace('/', '\\')
        prefs = {"download.default_directory" : download_dir, "directory_upgrade": True}
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(os.getenv('LETTERBOXD_SETTINGS_PAGE'))
        driver.implicitly_wait(10)
#         accept_cookies = driver.find_element(By.CLASS_NAME, 'fc-button-label')
#         accept_cookies.click()
        username = driver.find_element(By.XPATH, '//*[@id="signin-username"]')
        username.send_keys(os.getenv('LETTERBOXD_USER'))
        password = driver.find_element(By.XPATH, '//*[@id="signin-password"]')
        password.send_keys(os.getenv('LETTERBOXD_PASSWORD'))
        signin_button = driver.find_element(By.XPATH, '//*[@id="signin-form"]/fieldset/div[4]/div/input')
        signin_button.click()
        time.sleep(5)
        driver.get(page_url)
        page_source = driver.page_source
        return page_source
    except:
        return None