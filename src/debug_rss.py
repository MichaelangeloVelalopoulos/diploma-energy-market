import requests

url = "https://www.enexgroup.gr/el/web/guest/markets-publications-el-intraday-market?p_p_id=com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_Ibj5yiegpvGr&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=getRSS&p_p_cacheability=cacheLevelPage&_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_Ibj5yiegpvGr_cur=17&_com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_Ibj5yiegpvGr_delta=7"

r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"})
r.raise_for_status()
print(r.text[:2500])
