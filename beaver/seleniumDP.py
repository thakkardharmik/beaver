import logging
import time

logger = logging.getLogger(__name__)


class SeleniumDP(object):
    def __init__(self):
        pass

    @classmethod
    def click(cls, driver, clickable):
        try:
            driver.execute_script("return arguments[0].click();", clickable)
        except Exception:
            clickable.click()

    @classmethod
    def clickUsingClickable(cls, clickable):
        clickable.click()

    @classmethod
    def sendKeys(cls, field, key):
        inter_key_time = 0.1
        field.clear()
        for k in key:
            field.send_keys(k)
            time.sleep(inter_key_time)
