CREATE TABLE IF NOT EXISTS botMessages ( chatId text,firstName text , category text,offsetId text,fetchTime int(11), message text, userId text, username_user text, username_group text);

CREATE TABLE IF NOT EXISTS exchange_last_fetch (exchange text, country text, last_update int(11),present_time int(11), time_diff bigint(20));

CREATE TABLE IF NOT EXISTS coinmarketcap ( id text,name text,symbol text,rank int(11) ,price_usd decimal(18,9) ,price_btc decimal(18,9) ,24h_volume_usd decimal(38,9) ,market_cap_usd decimal(38,9) ,available_supply decimal(38,9) ,total_supply decimal(38,9) ,percent_change_1h decimal(18,9) ,percent_change_24h decimal(18,9) ,percent_change_7d decimal(18,9) ,last_updated text,fetchTime int(11) );

CREATE TABLE IF NOT EXISTS bittrex ( marketname text,high decimal(18,9) ,low decimal(18,9) ,volume decimal(38,9) ,last decimal(18,9) ,basevolume decimal(38,9) ,timestampp text,bid decimal(18,9) ,ask decimal(18,9) ,openbuyorders int(11) ,opensellorders int(11) ,prevday decimal(18,9) ,created text,fetchTime int(11) );
CREATE TABLE IF NOT EXISTS binance ( marketname text,price decimal(18,9),quoteVolume decimal(38,9),volume decimal(38,9) ,fetchTime int(11) );
CREATE TABLE IF NOT EXISTS idex ( marketname text,price decimal(18,9) ,fetchTime int(11) );
CREATE TABLE IF NOT EXISTS bitbns ( marketname text,price decimal(18,9) ,volume decimal(38,9), fetchTime int(11) );
CREATE TABLE IF NOT EXISTS zebpay ( marketname text,price decimal(18,9) ,volume decimal(38,9), fetchTime int(11) );
CREATE TABLE IF NOT EXISTS koinex ( marketname text,price decimal(18,9) ,volume decimal(38,9), fetchTime int(11) );
CREATE TABLE IF NOT EXISTS coindelta ( marketname text,price decimal(18,9) , fetchTime int(11) );
CREATE TABLE IF NOT EXISTS wazirx ( marketname text,price decimal(18,9) ,volume decimal(38,9), fetchTime int(11) );
CREATE TABLE IF NOT EXISTS unocoin ( marketname text,price decimal(18,9) ,volume decimal(38,9), fetchTime int(11) );
CREATE TABLE IF NOT EXISTS cryptopia ( marketname text,last_price decimal(18,9) ,fetchTime int(11) );
CREATE TABLE IF NOT EXISTS kucoin  ( symbol text,marketname text,price decimal(18,9) ,fetchTime int(11) );
CREATE TABLE IF NOT EXISTS bitfinex (marketname text,mid decimal(18,9),bid decimal(18,9),ask decimal(18,9),last_price decimal(18,9),low decimal(18,9),high decimal(18,9),volume decimal(38,9), timestampp text,fetchTime int);
CREATE TABLE IF NOT EXISTS poloniex (currencySymbol text,id text,name text,disabled int,delisted int,frozen int,fetchTime int);
CREATE TABLE IF NOT EXISTS tweets (tweet_id text,screen_name text, created_at text, inReplyToScreenName text,fetchTime int);
CREATE TABLE IF NOT EXISTS twitterMapping (coinmarketcap_id text , twitter_screen_name text);
CREATE TABLE IF NOT EXISTS free_coins (coin_name text,coin_symbol text,shorten_link text,complete_link text,official_website text,free_dollars int,free_coins int,added_time int,expiry_time int,last_fetchTime_notified int);

CREATE TABLE IF NOT EXISTS coinmarketcap_dn_ld AS SELECT  * FROM coinmarketcap LIMIT 0;
CREATE TABLE IF NOT EXISTS coinmarketcap_dn AS SELECT  * FROM coinmarketcap LIMIT 0;
CREATE TABLE IF NOT EXISTS coinmarketcap_BKP AS SELECT  * FROM coinmarketcap LIMIT 0;
CREATE TABLE IF NOT EXISTS coinmarketcap_t1 AS SELECT  * FROM coinmarketcap LIMIT 0;

CREATE TABLE IF NOT EXISTS bittrex_dn_ld AS SELECT  * FROM bittrex LIMIT 0;
CREATE TABLE IF NOT EXISTS bittrex_dn AS SELECT  * FROM bittrex LIMIT 0;
CREATE TABLE IF NOT EXISTS bittrex_BKP AS SELECT  * FROM bittrex LIMIT 0;
CREATE TABLE IF NOT EXISTS bittrex_t1 AS SELECT  * FROM bittrex LIMIT 0;

CREATE TABLE IF NOT EXISTS bitbns_dn_ld AS SELECT  * FROM bitbns LIMIT 0;
CREATE TABLE IF NOT EXISTS bitbns_dn AS SELECT  * FROM bitbns LIMIT 0;
CREATE TABLE IF NOT EXISTS bitbns_BKP AS SELECT  * FROM bitbns LIMIT 0;
CREATE TABLE IF NOT EXISTS bitbns_t1 AS SELECT  * FROM bitbns LIMIT 0;

CREATE TABLE IF NOT EXISTS zebpay_dn_ld AS SELECT  * FROM zebpay LIMIT 0;
CREATE TABLE IF NOT EXISTS zebpay_dn AS SELECT  * FROM zebpay LIMIT 0;
CREATE TABLE IF NOT EXISTS zebpay_BKP AS SELECT  * FROM zebpay LIMIT 0;
CREATE TABLE IF NOT EXISTS zebpay_t1 AS SELECT  * FROM zebpay LIMIT 0;

CREATE TABLE IF NOT EXISTS koinex_dn_ld AS SELECT  * FROM koinex LIMIT 0;
CREATE TABLE IF NOT EXISTS koinex_dn AS SELECT  * FROM koinex LIMIT 0;
CREATE TABLE IF NOT EXISTS koinex_BKP AS SELECT  * FROM koinex LIMIT 0;
CREATE TABLE IF NOT EXISTS koinex_t1 AS SELECT  * FROM koinex LIMIT 0;

CREATE TABLE IF NOT EXISTS coindelta_dn_ld AS SELECT  * FROM coindelta LIMIT 0;
CREATE TABLE IF NOT EXISTS coindelta_dn AS SELECT  * FROM coindelta LIMIT 0;
CREATE TABLE IF NOT EXISTS coindelta_BKP AS SELECT  * FROM coindelta LIMIT 0;
CREATE TABLE IF NOT EXISTS coindelta_t1 AS SELECT  * FROM coindelta LIMIT 0;

CREATE TABLE IF NOT EXISTS wazirx_dn_ld AS SELECT  * FROM wazirx LIMIT 0;
CREATE TABLE IF NOT EXISTS wazirx_dn AS SELECT  * FROM wazirx LIMIT 0;
CREATE TABLE IF NOT EXISTS wazirx_BKP AS SELECT  * FROM wazirx LIMIT 0;
CREATE TABLE IF NOT EXISTS wazirx_t1 AS SELECT  * FROM wazirx LIMIT 0;

CREATE TABLE IF NOT EXISTS unocoin_dn_ld AS SELECT  * FROM unocoin LIMIT 0;
CREATE TABLE IF NOT EXISTS unocoin_dn AS SELECT  * FROM unocoin LIMIT 0;
CREATE TABLE IF NOT EXISTS unocoin_BKP AS SELECT  * FROM unocoin LIMIT 0;
CREATE TABLE IF NOT EXISTS unocoin_t1 AS SELECT  * FROM unocoin LIMIT 0;

CREATE TABLE IF NOT EXISTS idex_dn_ld AS SELECT  * FROM idex LIMIT 0;
CREATE TABLE IF NOT EXISTS idex_dn AS SELECT  * FROM idex LIMIT 0;
CREATE TABLE IF NOT EXISTS idex_BKP AS SELECT  * FROM idex LIMIT 0;
CREATE TABLE IF NOT EXISTS idex_t1 AS SELECT  * FROM idex LIMIT 0;

CREATE TABLE IF NOT EXISTS hitbtc_dn_ld AS SELECT  * FROM hitbtc LIMIT 0;
CREATE TABLE IF NOT EXISTS hitbtc_dn AS SELECT  * FROM hitbtc LIMIT 0;
CREATE TABLE IF NOT EXISTS hitbtc_BKP AS SELECT  * FROM hitbtc LIMIT 0;
CREATE TABLE IF NOT EXISTS hitbtc_t1 AS SELECT  * FROM hitbtc LIMIT 0;

CREATE TABLE IF NOT EXISTS binance_dn_ld AS SELECT  * FROM binance LIMIT 0;
CREATE TABLE IF NOT EXISTS binance_dn AS SELECT  * FROM binance LIMIT 0;
CREATE TABLE IF NOT EXISTS binance_BKP AS SELECT  * FROM binance LIMIT 0;
CREATE TABLE IF NOT EXISTS binance_t1 AS SELECT  * FROM binance LIMIT 0;

CREATE TABLE IF NOT EXISTS kucoin_dn_ld AS SELECT  * FROM kucoin LIMIT 0;
CREATE TABLE IF NOT EXISTS kucoin_dn AS SELECT  * FROM kucoin LIMIT 0;
CREATE TABLE IF NOT EXISTS kucoin_BKP AS SELECT  * FROM kucoin LIMIT 0;
CREATE TABLE IF NOT EXISTS kucoin_t1 AS SELECT  * FROM kucoin LIMIT 0;

CREATE TABLE IF NOT EXISTS cryptopia_dn_ld AS SELECT  * FROM cryptopia LIMIT 0;
CREATE TABLE IF NOT EXISTS cryptopia_dn AS SELECT  * FROM cryptopia LIMIT 0;
CREATE TABLE IF NOT EXISTS cryptopia_BKP AS SELECT  * FROM cryptopia LIMIT 0;
CREATE TABLE IF NOT EXISTS cryptopia_t1 AS SELECT  * FROM cryptopia LIMIT 0;

CREATE TABLE IF NOT EXISTS tweets_dn_ld AS SELECT  * FROM tweets LIMIT 0;
CREATE TABLE IF NOT EXISTS tweets_dn AS SELECT  * FROM tweets LIMIT 0;
CREATE TABLE IF NOT EXISTS tweets_BKP AS SELECT  * FROM tweets LIMIT 0;
CREATE TABLE IF NOT EXISTS tweets_t1 AS SELECT  * FROM tweets LIMIT 0;

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
CREATE TABLE IF NOT EXISTS  alerts_subscription_dn_ld_t1 AS SELECT  * FROM alerts_subscription_dn_ld LIMIT 0;

CREATE TABLE IF NOT EXISTS  send_alerts (alert_number int NOT NULL AUTO_INCREMENT,id int,chatId text , alert_type text, new_alert_fetchTime int , coin_symbol text,is_first text,alert_price decimal(18,9),price_in text,twitter_screen_name text,tweet_id text,coin_id text, coin_name text,exchange text,new_price decimal(18,9), PRIMARY KEY (alert_number));