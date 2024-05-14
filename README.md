# FBRef Scraper



### Description

FBRef Scraper is a Python package designed to scrape data from the website FBRef. It utilizes Poetry for package management and Typer for building a command-line interface to facilitate scraping.

#### Installation

Install this package inside a Virtual Environment :

```bash
# pip
pip install football-analytics

#poetry
poetry add football-analytics
```

Usage

To run the scraper, use the following command:

```bash
fbref-scrap COMMAND
```

Replace COMMAND with the specific functionality you want to use.

Available Commands
* main: Initiates the scraping process. It's instantiated in a callback function, so no need to precise "main" when calling fbref-scrap command.
* Add more commands here as you develop additional functionality.


#### Precision on the main command

Docstring to the main function :
```python
Typer command running the main function, called scraping_data_games.

Args:
    start_date (str, optional): first date to be scrapped, should be YYYYMMDD. Defaults to "20240101".
    end_date (Union[str, None], optional): last date to be scrapped, should be YYYYMMDD. Defaults to None.
    db_config_path (Union[str, None], optional): Path to the config file, should be a .ini file with a [postgresql] header. If filed, we consider you want to save the output to the related database. Defaults to None.
```

Let's provide some examples on how you can run it.


```bash
# Run the scraper on January 1st 2024 (default date) only, no output are saved.
fbref-scrap

# Run the scraper on May 6th 2024 only, no output are saved.
fbref-scrap --start-date 20240506

# Run the scraper on all dates from May 6th 2024 to May 13th 2024.
fbref-scrap --start-date 20240506 --end-date 20240513

# Run the scraper on all dates from May 6th 2024 to May 13th 2024, then look to a config file called database.ini (relatively to where you are when calling the scraper) to export the output to a PostGreSQL database
fbref-scrap --start-date 20240506 --end-date 20240513 --db-config-path database.ini
```


### Configuration

The only configuration you might need is creating a PostGreSQL database to export the results of the main command. If you don't have a PostgreSQL setup already, you can do the following :

* Install PostGreSQL. You can follow this tutorial which is nice and simple : https://www.youtube.com/watch?v=PShGF_udSpk
* Create a database manually, you can call it `FootballAnalytics` for example.
* Create a ini file, it can be named `database.ini`.
* Push to the branch (git push origin feature/your-feature).
* Create a new Pull Request.

Expected template of the ini file :
```ini
[postgresql]
host=localhost
database=FootballAnalytics
user=your_psql_user_name
password=your_psql_password
```


### Contributing

Contributions are welcome! Please follow these steps:

* Fork the repository.
* Create your feature branch (git checkout -b feature/your-feature).
* Commit your changes (git commit -am 'Add some feature').
* Push to the branch (git push origin feature/your-feature).
* Create a new Pull Request.

### License

This project is licensed under the MIT License.
