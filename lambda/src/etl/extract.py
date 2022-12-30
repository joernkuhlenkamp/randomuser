import json
from requests import (  Response,
                        Session, )
from requests.adapters import ( HTTPAdapter, 
                                Retry, )

class ExtractionError(RuntimeError):
    def __init__(self, message):
        super().__init__(message)

def _get_session() -> Session:
    session = Session()

    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[ 500, 502, 503, 504 ]
    )

    session.mount('https://', HTTPAdapter(max_retries=retries))

    return session

def _fetch_users(session: Session, limit: int = 1, seed: str = '') -> Response:
    return session.get(
        url='https://randomuser.me/api',
        headers={},
        params={
            'inc': 'gender,login,location,email',
            'results': limit,
            'seed': seed,
        },
        timeout=10,
    )

def _except_errors_of_fetch_users(limit: int = 1, seed: str = '') -> Response:
    session = _get_session()
    try:
        users = _fetch_users(session, limit, seed)
    except Exception as e:
        session.close()
        raise ExtractionError(f'Failed fetching remote user records. {str(e)}')
    session.close()
    return users

def extract(limit: int = 1, seed: str = '') -> list[dict]:
    response_json = _except_errors_of_fetch_users(limit, seed)
    response = json.loads(response_json.text)
    return response["results"]