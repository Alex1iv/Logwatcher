import requests
from requests.exceptions import RequestException, Timeout, HTTPError
from requests.auth import HTTPBasicAuth


def download_csv(url:str, credentials:dict, dest:str, timeout:int=10,logger=None
    # username:str, # password: str, #LOCAL_FILENAME = "ports.csv",
    ) -> bool:
    """Загружаем файл ports.csv с удаленного адреса

    Args:
        url (str): http адрес сервера
        credentials (dict): логин и пароль к серверу
        dest (Path): папка для скачивания файла ports.csv. Defaults to "/tmp/".
        timeout (int): время ожидания соединения с сервером. Defaults to 10.

    Returns:
        bool: True - если файл скачался успешно; False - не скачался
    """    
    try:
        response = requests.get(
            url,
            auth=HTTPBasicAuth(credentials["PORTS_USER"], credentials["PORTS_PASSWORD"]), 
            timeout=timeout)
        response.raise_for_status()  # raises HTTPError on failure
        
        with open(dest, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        if logger:
            logger.info(f"File successfully downloaded and saved to {dest}")
        
        return True

    except Timeout:
        print(f"[ERROR] Timeout while downloading file from {url}")
    except HTTPError as e:
        print(f"[ERROR] HTTP error while downloading file: {e}")
    except RequestException as e:
        print(f"[ERROR] Network-related error: {e}")
    except OSError as e:
        print(f"[ERROR] File system error while saving {dest}: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}") 
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Unexpected error: {e}")
        
    if logger:
        logger.error(f"[DOWNLOAD] Failure to download ports.csv")
        
    return False