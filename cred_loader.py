import yaml
import os

def load_credentials(args, paths, logger=None):
    cred = {}
    if args.remote:
        with open(paths["SECRETS_PATH"]) as f:
            secrets = yaml.safe_load(f) or {}
        
        for i in ['PORTS_USER','PORTS_PASSWORD', 'POSTGRES_USER', 'POSTGRES_PASSWORD']:
            cred[i] = secrets.get(i)

    else:
        # for i in ['PORTS_USER','PORTS_PASSWORD', 'POSTGRES_USER', 'POSTGRES_PASSWORD']:
        #     cred[i] = os.environ.get(i)
        cred['PORTS_USER'] = os.environ.get('PORTS_USER')
        cred['PORTS_PASSWORD'] = os.environ.get('PORTS_PASSWORD')
        cred['POSTGRES_USER'] = os.environ.get('POSTGRES_USER')
        cred['POSTGRES_PASSWORD'] = os.environ.get('POSTGRES_PASSWORD')
            
    if not cred['PORTS_USER'] or not cred["PORTS_PASSWORD"]:
        if logger:
            logger.warning("[CRED] Missing credentials for ports.csv")
        raise RuntimeError("Missing credentials for ports.csv")
    
    elif not cred['POSTGRES_USER'] or not cred["POSTGRES_PASSWORD"]:
        if logger:
            logger.warning("[CRED] Missing credentials for Postgresql")
        raise RuntimeError("Missing credentials for Postgresql")

    return cred