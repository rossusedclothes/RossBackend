import json

from cozepy import COZE_CN_BASE_URL

from apps.rebot.panel import crud, schemas
from core.database import db_getter

coze_api_base = COZE_CN_BASE_URL

from cozepy import Coze, TokenAuth, Message, ChatStatus, MessageContentType  # noqa
from loguru import logger


class CozeApi:

    def __init__(self, COZE_TOKEN=None):
        self.coze = Coze(auth=TokenAuth(token=COZE_TOKEN), base_url=coze_api_base)

    def send_foreign_trade_workflow(self, message: str, user_id: str | int, sales_repp_phone: str, sales_agent: str,
                                    platform: str = "message"):
        """

        """
        workflow = self.coze.workflows.runs.create(
            workflow_id='7568320907665309722',
            parameters={
                "input": message,
                "user_id": user_id,
                "sales_repp_phone": sales_repp_phone or '0',
                "sales_agent": sales_agent or 'Ross Company Intelligent Assistant',
                "platform": platform or "message"
            })
        logger.info(workflow)
        data = json.loads(workflow.data)
        output = data.get("output")
        user_info = data.get("userInfo")
        if not output:
            # TODO 响应失败请联系客户：user_id
            return ""
        return output, user_info


if __name__ == '__main__':
    coze_api = CozeApi(COZE_TOKEN="pat_upP8xqabv9aUHSHDYu8xJN3S91BWBP8BG70IcUiGfP2bsgnpAOOjL1o41M1i6Dmu")
    output, user_info = coze_api.send_foreign_trade_workflow("hello", "17754576486", "1", "1")
    print(output)
