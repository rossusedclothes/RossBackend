from core.coze_api import CozeApi

if __name__ == '__main__':
    import uvicorn

    coze = CozeApi(COZE_TOKEN="pat_upP8xqabv9aUHSHDYu8xJN3S91BWBP8BG70IcUiGfP2bsgnpAOOjL1o41M1i6Dmu")
    result, user_info = coze.send_foreign_trade_workflow("test", "test_id", "test_phone", "test_agent")
    print(result)
