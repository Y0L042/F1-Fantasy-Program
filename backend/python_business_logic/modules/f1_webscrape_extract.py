from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

url = 'https://www.f1fantasytracker.com/prices.html'

# Set up Selenium options
options = Options()
# options.add_argument("--headless")
# options.add_argument("--disable-infobars")
# options.add_argument("--disable-extensions")
# options.add_argument("--no-sandbox")


def TODO_SORT_INTO_FUNCTIONS():
	# Specify the path to your webdriver (in this example, I'm using Chrome)
	driver = webdriver.Chrome(options=options)

	# Fetch the page and load it with JavaScript
	driver.get(url)

	# Get the page source (including JavaScript generated content)
	page_source = driver.page_source

	# Close the Selenium driver
	driver.quit()

	# Parse the HTML content with BeautifulSoup
	soup = BeautifulSoup(page_source, 'html.parser')

	# Find the table body
	table_body = soup.find('tbody')

	# Get all table rows
	table_rows = table_body.find_all('tr', role='row') # type: ignore

	# Extract and print data
	print("Team Name | Current Price | Season Start Price | Points/Million")
	for row in table_rows:
		# Get team name
		team_name = row.find('div', class_='subname').text.strip()

		# Get current price
		current_price = row.find('span', {'id': lambda x: x and x.startswith('CurrentPrice')}).text.strip()

		# Get season start price
		season_start_price = row.find('span', {'id': lambda x: x and x.startswith('SeasonPrice')}).text.strip()

		# Get points per million
		points_per_million = row.find('td', {'id': lambda x: x and x.startswith('Million')}).text.strip()

		print(f"{team_name} | ${current_price}m | ${season_start_price}m | {points_per_million}")


	# Find the table containing the drivers' data
	drivers_table_body = soup.select_one('table#drivers')

	# Get all table rows for drivers
	drivers_table_rows = drivers_table_body.find_all('tr', role='row') # type: ignore

	# Extract and print drivers data
	print("Driver Name | Current Price | Season Start Price | Points/Million")
	for row in drivers_table_rows:
		columns = row.find_all('td')
		driver_name = columns[0].text.strip()
		current_price = columns[1].text.strip()
		season_start_price = columns[2].text.strip()
		points_per_million = columns[3].text.strip()

		print(f"{driver_name} | {current_price} | {season_start_price} | {points_per_million}")



	# ChatGPT 4 Example. To be used as a reference.