from __future__ import print_function
import aylien_news_api
from aylien_news_api.rest import ApiException
from pprint import pprint

configuration = aylien_news_api.Configuration()
configuration.api_key['X-AYLIEN-NewsAPI-Application-ID'] = 'YOUR_APP_ID'
configuration.api_key['X-AYLIEN-NewsAPI-Application-Key'] = 'YOUR_APP_KEY'
# Most programs detect that they must use a proxy by checking the http_proxy / https_proxy environment variables.
# This is not the case for the Aylien SDK therefore you need to specify the proxy in the code using configuration.proxy.
# Update the value below with your HTTP or HTTPS proxy variable as provided by your IT team
configuration.proxy = 'YOUR_PROXY_SERVER_URL'

client = aylien_news_api.ApiClient(configuration)
api_instance = aylien_news_api.DefaultApi(client)

try:
    api_response = api_instance.list_stories(
        title='startup',
        published_at_start='NOW-7DAYS',
        published_at_end='NOW'
    )
    pprint(api_response)
except ApiException as e:
    print("Exception when calling DefaultApi->list_stories: %s\n" % e)
