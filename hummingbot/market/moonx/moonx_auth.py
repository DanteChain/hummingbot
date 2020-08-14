import hashlib
import time
import uuid
from typing import Any, Dict


class MoonxAuth:

    def __init__(self, business_num: str, api_secret: str):
        self.api_secret = api_secret
        self.business_num = business_num

    def sign_it(self, input_params: Dict[str, Any]):
        nonce = str(uuid.uuid1())
        timestamp = int(round(time.time()))

        jsonData: Dict[str, Any] = {
            "nonceStr": nonce,
            "timestamp": timestamp,
            "apiSecret": self.api_secret
        }
        jsonData.update(input_params)
        result: str = '&'.join('{}={}'.format(k, jsonData[k]) for k in sorted(jsonData))

        sign: str = str(hashlib.md5(result.encode()).hexdigest()).upper()

        return {
            "businessNo": self.business_num,
            "nonceStr": nonce,
            "timestamp": timestamp,
            "data": input_params,
            "sign": sign
        }
