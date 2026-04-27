# tap-workday-raas

[![PyPI version](https://badge.fury.io/py/tap-mysql.svg)](https://badge.fury.io/py/tap-workday-raas)
[![CircleCI Build Status](https://circleci.com/gh/singer-io/tap-workday-raas.png)](https://circleci.com/gh/singer-io/tap-workday-raas.png)


[Singer](https://www.singer.io/) tap that extracts data from a [Workday](https://www.workday.com/) report and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

```bash
$ mkvirtualenv -p python3 tap-workday-raas
$ pip install tap-workday-raas
$ tap-workday-raas --config config.json --discover
$ tap-workday-raas --config config.json --properties properties.json --state state.json
```

# Quickstart

Ensure poetry is installed on your machine. 

- This command will return the installed version of poetry if it is installed.
```
poetry --version
```

- If not, install poetry using the following commands (from https://python-poetry.org/docs/#installation):
```
curl -sSL https://install.python-poetry.org | python3 -
PATH=~/.local/bin:$PATH
```

Within the `tap-workday-raas` directory, install dependencies:
```
poetry install
```

Then run the tap:
```
poetry run tap-workday-raas <options>
```
## Create Config

   Copy **`sample_config.json`** (username/password) or **`sample_config.oauth.json`** (OAuth) as a starting point for your `config.json`.

   The tap config file for this tap should include these entries:

   - `username` - The username of the workday account with access to the reports to extract
   - `password` - The password of the workday account with access to the reports to extract
   - `reports` -  An array containing a list of objects containing the `report_name` and `report_url`. `report_name` is the name of the stream for the report, and the `report_url` is the URL to the Workday XML REST link for the report you wish to extract.

   ```json
   {
       "username": "<username>",
       "password": "<password>",
       "reports": [{"report_name": "abitrary_name", "report_url": "https://..."},]
   }
   ```

### OAuth (alternative to username / password)

Use an **API Client for Integrations** from Workday. Typical keys: `auth_type`: `"oauth"`, `client_id`, `client_secret`, `token_url` (`https://{your-host}/ccx/oauth2/{tenant}/token`), `oauth_grant_type` (`client_credentials` or `refresh_token`), optional `oauth_scope`, optional `refresh_token` for the refresh grant.

**Grant type must match the Workday client.** If the client uses **Authorization Code** (you have a **refresh token**), set **`oauth_grant_type` to `refresh_token`** and supply **`refresh_token`**. Do **not** pair `client_credentials` with a refresh token in config—Workday often returns HTTP 400. Use **`client_credentials`** only for clients registered for that grant with **no** `refresh_token` in config.

**HTTP 401 `invalid_client`:** Wrong secret or wrong **client id** string for the token endpoint. Workday may expect the ID as shown (Base64) or the **decoded UUID**; with **`oauth_client_id_format`**: `auto` (default), the tap uses the resolved UUID first on **refresh_token** and retries with the raw value if Workday returns `invalid_client`. Use **`raw`** or **`uuid`** to force one encoding.

If the token endpoint returns **HTTP 400** while credentials look correct, try **`oauth_token_client_auth": "post_body"`** so `client_id` / `client_secret` are sent in the form body instead of the `Authorization` header.

```json
{
  "auth_type": "oauth",
  "client_id": "<api_client_id>",
  "client_secret": "<secret>",
  "token_url": "https://impl-cc.workday.com/ccx/oauth2/your_tenant/token",
  "oauth_grant_type": "client_credentials",
  "reports": [{"report_name": "my_report", "report_url": "https://..."}]
}
```

Start from **`sample_config.oauth.json`** for `client_credentials`. For **Authorization Code** / refresh tokens, use the same file shape but set **`oauth_grant_type`** to **`refresh_token`**, add **`refresh_token`**, and remove any stray **`refresh_token`** when you stay on **`client_credentials`** (validation rejects mixing them).

## Run Discovery

To run discovery mode, execute the tap with the config file.

```
> tap-workday-raas --config config.json --discover > properties.json
```

## Sync Data

To sync data, select fields in the catalog (or legacy properties) JSON from discover and run the tap. **`--catalog`** is preferred; **`-p` / `--properties`** still works and is treated as catalog JSON.

```
> tap-workday-raas --config config.json --catalog catalog.json [--state state.json]
> tap-workday-raas -c config.json -p catalog.json [--state state.json]
```

## Package manager

We only use poetry to manage our packages. Pipfile is there because our code scan doesn't support poetry.lock. So we do the following hack to generate Pipfile and Pipfile.lock based on our poetry.lock:
# 1. Export all dependencies from poetry.lock to requirements.txt
```
poetry export -f requirements.txt --output requirements.txt --without-hashes
```
# 1b. (Optional) Make sure pipenv has the right python version
Check:
```
pipenv --support
```
Install:
```
python -m pip install --user pipenv
```

# 2. Generate Pipfile and Pipfile.lock from requirements.txt (make sure you pass in right version of python)
```
pipenv install --python 3.13 -r requirements.txt
```

Check that the required python version in the Pipfile matches your expected python version. For some reason even if requirements.txt specify the right python version pipenv can still default to a different version based on the some stale versioning in the venv. In which case, do the following:

# 1. Delete the Pipfile and lock, and deactivate your venv

# 2. Delete the venv with `pipenv --rm`

# 3. Re-run the pipenv install command

Copyright &copy; 2020 Stitch
