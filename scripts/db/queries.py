import scripts.db.requests as requests
import scripts.db.providers as providers

requests_join_providers = f'{requests.Table} join {providers.Table} on {requests.cid} = {providers.cid}'
