                      YouTube-Data-Harvesting-and-Warehousing

YouTube Data Harvesting and Warehousing involves collecting, storing, and organizing data from YouTube for various purposes, such as analysis, research, or business intelligence. To create a Streamlit application we need to access and analyze data from multiple YouTube channels. 

features:
* To input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
* To collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
* To store the data in a MYSQL.
* To search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

Approach:

*Set up a Streamlit: Streamlit is for building data visualization and analysis tools quickly and easily. You can use Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.

*Connect to the YouTube API: Need to use the YouTube API to retrieve channel and video data. You can use the Google API client library for Python to make requests to the API.

*Store and Clean data : Once you retrieve the data from the YouTube API, store it in a suitable format for temporary storage before migrating to the data warehouse.

*Migrate data to a SQL data warehouse: After collected data for multiple channels,can migrate it to a SQL data warehouse.

*Query the SQL data warehouse: SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.

*Display data in the Streamlit: Finally,display the retrieved data in the Streamlit app. 
