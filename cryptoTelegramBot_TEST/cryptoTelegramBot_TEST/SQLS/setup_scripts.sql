CREATE TABLE IF NOT EXISTS botMessages ( chatId text,firstName text , category text,offsetId text,fetchTime int(11), message text);

CREATE TABLE IF NOT EXISTS coinmarketcap ( id text,name text,symbol text,rank int(11) ,price_usd decimal(18,9) ,price_btc decimal(18,9) ,24h_volume_usd decimal(38,9) ,market_cap_usd decimal(38,9) ,available_supply decimal(38,9) ,total_supply decimal(38,9) ,percent_change_1h decimal(18,9) ,percent_change_24h decimal(18,9) ,percent_change_7d decimal(18,9) ,last_updated text,fetchTime int(11) );

CREATE TABLE IF NOT EXISTS bittrex ( marketname text,high decimal(18,9) ,low decimal(18,9) ,volume decimal(38,9) ,last decimal(18,9) ,basevolume decimal(38,9) ,timestampp text,bid decimal(18,9) ,ask decimal(18,9) ,openbuyorders int(11) ,opensellorders int(11) ,prevday decimal(18,9) ,created text,fetchTime int(11) );
CREATE TABLE IF NOT EXISTS bitfinex (marketname text,mid decimal(18,9),bid decimal(18,9),ask decimal(18,9),last_price decimal(18,9),low decimal(18,9),high decimal(18,9),volume decimal(38,9), timestampp text,fetchTime int);
CREATE TABLE IF NOT EXISTS poloniex (currencySymbol text,id text,name text,disabled int,delisted int,frozen int,fetchTime int);
CREATE TABLE IF NOT EXISTS tweets (tweet_id text,screen_name text, created_at text, tweet text, inReplyToScreenName text,fetchTime int);
CREATE TABLE IF NOT EXISTS twitterMapping (coinmarketcap_id text , twitter_screen_name text);
CREATE TABLE IF NOT EXISTS free_coins (coin_name text,coin_symbol text,shorten_link text,complete_link text,official_website text,free_dollars int,free_coins int,added_time int,expiry_time int,last_fetchTime_notified int);

CREATE TABLE IF NOT EXISTS coinmarketcap_dn_ld AS SELECT  * FROM coinmarketcap LIMIT 0;
CREATE TABLE IF NOT EXISTS coinmarketcap_dn AS SELECT  * FROM coinmarketcap LIMIT 0;
CREATE TABLE IF NOT EXISTS coinmarketcap_BKP AS SELECT  * FROM coinmarketcap LIMIT 0;
CREATE TABLE IF NOT EXISTS coinmarketcap_t1 AS SELECT id,fetchTime from coinmarketcap LIMIT 0;

CREATE TABLE IF NOT EXISTS bittrex_dn_ld AS SELECT  * FROM bittrex LIMIT 0;
CREATE TABLE IF NOT EXISTS bittrex_dn AS SELECT  * FROM bittrex LIMIT 0;
CREATE TABLE IF NOT EXISTS bittrex_BKP AS SELECT  * FROM bittrex LIMIT 0;
CREATE TABLE IF NOT EXISTS bittrex_t1 AS SELECT marketname,fetchTime from bittrex LIMIT 0;

CREATE TABLE IF NOT EXISTS bitfinex_dn_ld AS SELECT  * FROM bitfinex LIMIT 0;
CREATE TABLE IF NOT EXISTS bitfinex_dn AS SELECT  * FROM bitfinex LIMIT 0;
CREATE TABLE IF NOT EXISTS bitfinex_BKP AS SELECT  * FROM bitfinex LIMIT 0;
CREATE TABLE IF NOT EXISTS bitfinex_t1 AS SELECT marketname,fetchTime from bitfinex LIMIT 0;

CREATE TABLE IF NOT EXISTS  price_denorm_BKP ( rank int(11), id text, symbol text, name text, twitter_screen_name text,tweet_id text,  tweet_fetchTime int , exchange text, marketname text, exchange_last_price decimal(18,9),exchange_last_price_in text, cmc_price_usd decimal(18,9) , cmc_price_btc decimal(18,9), cmc_24h_volume_usd decimal(38,9),cmc_market_cap_usd decimal(38,9),cmc_percent_change_1h decimal(18,9), cmc_percent_change_24h decimal(18,9), cmc_percent_change_7d decimal(18,9),is_new_market text, created_at int ) ;
CREATE TABLE IF NOT EXISTS  price_denorm_t1 ( rank int(11), id text, symbol text, name text, twitter_screen_name text,tweet_id text,  tweet_fetchTime int , exchange text, marketname text, exchange_last_price decimal(18,9),exchange_last_price_in text, cmc_price_usd decimal(18,9) , cmc_price_btc decimal(18,9), cmc_24h_volume_usd decimal(38,9),cmc_market_cap_usd decimal(38,9),cmc_percent_change_1h decimal(18,9), cmc_percent_change_24h decimal(18,9), cmc_percent_change_7d decimal(18,9),is_new_market text, created_at int ) ;
CREATE TABLE IF NOT EXISTS  price_denorm_t2 ( rank int(11), id text, symbol text, name text, twitter_screen_name text,tweet_id text,  tweet_fetchTime int , exchange text, marketname text, exchange_last_price decimal(18,9),exchange_last_price_in text, cmc_price_usd decimal(18,9) , cmc_price_btc decimal(18,9), cmc_24h_volume_usd decimal(38,9),cmc_market_cap_usd decimal(38,9),cmc_percent_change_1h decimal(18,9), cmc_percent_change_24h decimal(18,9), cmc_percent_change_7d decimal(18,9),is_new_market text, created_at int ) ;
CREATE TABLE IF NOT EXISTS  price_denorm_ld ( rank int(11), id text, symbol text, name text, twitter_screen_name text,tweet_id text,  tweet_fetchTime int , exchange text, marketname text, exchange_last_price decimal(18,9),exchange_last_price_in text, cmc_price_usd decimal(18,9) , cmc_price_btc decimal(18,9), cmc_24h_volume_usd decimal(38,9),cmc_market_cap_usd decimal(38,9),cmc_percent_change_1h decimal(18,9), cmc_percent_change_24h decimal(18,9), cmc_percent_change_7d decimal(18,9),is_new_market text, created_at int ) ;
CREATE TABLE IF NOT EXISTS  price_denorm    ( rank int(11), id text, symbol text, name text, twitter_screen_name text,tweet_id text,  tweet_fetchTime int , exchange text, marketname text, exchange_last_price decimal(18,9),exchange_last_price_in text, cmc_price_usd decimal(18,9) , cmc_price_btc decimal(18,9), cmc_24h_volume_usd decimal(38,9),cmc_market_cap_usd decimal(38,9),cmc_percent_change_1h decimal(18,9), cmc_percent_change_24h decimal(18,9), cmc_percent_change_7d decimal(18,9),is_new_market text, created_at int ) ;

CREATE TABLE IF NOT EXISTS  alerts_subscription (id int NOT NULL AUTO_INCREMENT,chatId text , alert_type text, alert_fetchTime int , coin_symbol text,is_first text,alert_price decimal(18,9),price_in text, PRIMARY KEY (ID));
CREATE TABLE IF NOT EXISTS  alerts_subscription_BKP (id int ,chatId text , alert_type text, alert_fetchTime int , coin_symbol text,is_first text,alert_price decimal(18,9),price_in text);
CREATE TABLE IF NOT EXISTS  alerts_subscription_dn (id int,chatId text , alert_type text, alert_fetchTime int , coin_symbol text,is_first text,alert_price decimal(18,9),price_in text);
CREATE TABLE IF NOT EXISTS  alerts_subscription_dn_ld (id int,chatId text , alert_type text, alert_fetchTime int , coin_symbol text,is_first text,alert_price decimal(18,9),price_in text);
CREATE TABLE IF NOT EXISTS  alerts_subscription_t1 (id int, alert_fetchTime int);

CREATE TABLE IF NOT EXISTS  send_alerts (alert_number int NOT NULL AUTO_INCREMENT,id int,chatId text , alert_type text, new_alert_fetchTime int , coin_symbol text,is_first text,alert_price decimal(18,9),price_in text,twitter_screen_name text,tweet_id text,coin_id text, coin_name text,exchange text,new_price decimal(18,9), PRIMARY KEY (alert_number));