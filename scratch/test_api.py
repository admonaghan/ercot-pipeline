import requests
import os
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
SUBSCRIPTION_KEY = os.getenv("SUBSCRIPTION_KEY")

# Authorization URL for signing into ERCOT Public API account
AUTH_URL = (
    "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token\
?username={username}\
&password={password}\
&grant_type=password\
&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access\
&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70\
&response_type=id_token"
)

# Sign In/Authenticate
auth_response = requests.post(AUTH_URL.format(username=USERNAME, password=PASSWORD))

# Retrieve access token
access_token = auth_response.json().get("access_token")

print(access_token)
