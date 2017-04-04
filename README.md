Steps to get project up and running:

1) Move the data into PLAY BY PLAY DATA into hadoop:

    For, play by play files:
    VM: /inputs/baseball/[year]/[file by year]
    Cloud: /inputs/jakecoll/baseball/[year]/[file by year]
    
    For: player_names_and_ids.csv
    VM: /inputs/baseball/player_ids
    Cloud: /inputs/jakecoll/baseball/player_ids
    
    Note: The data comes from retrosheet.org. They have several packages to extract csv from there event files, but it requires windows.
    
2) Create tables in HBase:

    create 'jakecoll_batter_v_pitcher_2015', 'stats'
    create 'jakecoll_batter_v_pitcher_2014', 'stats'
    create 'jakecoll_batter_v_pitcher_2013', 'stats'

3) Run batter_v_pitcher.pig. File is set to run on cluster. If you want to run locally uncomment VM settings:

    EX: pig -param year=2015 batter_v_pitcher.pig
    
4) Create 'jakecoll-bvp-events' kafka topic:

    
5) In web server move css, html files to /var/www/html and perl files to /usr/lib/cgi-bin and update read-write permissions:


6) Run the jakecoll-bvp-topology to update any submitted data. Submitted data should update the 2015 stat line on the web app. 
