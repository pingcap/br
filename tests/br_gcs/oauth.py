from flask import Flask

app = Flask(__name__)

@app.route('/oauth/token', methods=['GET', 'POST'])
def oauth():
    return '{"access_token": "ok", "token_type":"service_account", "expires_in":3600}'
