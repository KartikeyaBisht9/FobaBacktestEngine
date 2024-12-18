from foba_backtest_engine.data.S3.S3Backbone import (
    OPTIVER_BUCKET_DETAILS,
    OPTIVER_BUCKET_NAME,
)

OPTIVER_AWS_RESOURCE = OPTIVER_BUCKET_DETAILS.create_resource()
OPTIVER_BUCKET_ACTIONS = OPTIVER_BUCKET_DETAILS.bucket_actions(
    bucket_name=OPTIVER_BUCKET_NAME
)
