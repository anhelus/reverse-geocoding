import pandas as pd
import geopy
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time
import sys

geopy.geocoders.options.default_timeout = 10

locator = Nominatim(user_agent="myGeocoder")

df = pd.read_excel('dt.xlsx')

addresses = []
for idx, row in df.iterrows():
	addresses.append({'street': row['Indirizzo'], 'city': row['Comune'], 'country': 'Italy'})

df_out = pd.DataFrame(columns=['lat', 'lon', 'addr'])

offset = 38844
# batch_size = 5000
batch_size = len(addresses) - offset

count = 6
# while offset < len(addresses):
max_attempts = 1
for i in range(offset, len(addresses)):
	time.sleep(0.2)
	try:
		location = locator.geocode(addresses[i])

		if location:
			df_out.loc[i] = [location.latitude] + [location.longitude] + [addresses[i]['street']]
		else:
			df_out.loc[i] = [pd.NA] + [pd.NA] + [addresses[i]['street']]
		
	except geopy.exc.GeocoderTimedOut:
		sys.stdout.write('Geocoder timeout\r')
		sys.stdout.flush()
		time.sleep(15)
		max_attempts += 1
		if max_attempts >= 7:
			with open('errors.txt', 'a') as f:
				f.write('Values not formatted: {} - {} - Refer to the {}-th file.\r'.format(offset, offset + batch_size, count))
				f.close()
			break
	
	except geopy.exc.GeocoderServiceError:
		df_out.loc[i] = [pd.NA] + [pd.NA] + [addresses[i]['street']]
			
	sys.stdout.write('{} % completed...\r'.format(round((i-offset) * 100 / batch_size, 2)))
	sys.stdout.flush()

df_out.to_excel('out_{}.xlsx'.format(count))
