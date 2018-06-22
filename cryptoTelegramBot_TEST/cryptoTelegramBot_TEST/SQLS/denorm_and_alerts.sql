DROP TABLE IF EXISTS exchange_last_fetch;
CREATE TABLE IF NOT EXISTS exchange_last_fetch AS
select "Coinmarketcap" AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  coinmarketcap_dn    UNION ALL
select "Binance"       AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  binance_dn    UNION ALL
select "Bittrex"       AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  bittrex_dn    UNION ALL
select "Kucoin"        AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  kucoin_dn    UNION ALL
select "Cryptopia"     AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  cryptopia_dn    UNION ALL
select "Hitbtc"        AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  hitbtc_dn    UNION ALL
select "Idex"          AS exchange,"Global" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  idex_dn    UNION ALL
select "Bitbns"    AS exchange,"Indian" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  bitbns_dn    UNION ALL
select "Zebpay"    AS exchange,"Indian" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  zebpay_dn    UNION ALL
select "Koinex"    AS exchange,"Indian" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  koinex_dn    UNION ALL
select "Coindelta" AS exchange,"Indian" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  coindelta_dn UNION ALL
select "WazirX"    AS exchange,"Indian" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  wazirx_dn    UNION ALL
select "Unocoin"   AS exchange,"Indian" AS country,max(fetchTime) AS last_update,unix_timestamp() AS present_time,unix_timestamp()-max(fetchTime) AS time_diff from  unocoin_dn ;


INSERT INTO price_denorm_BKP SELECT * FROM price_denorm;

drop table if exists price_denorm_temp1;
drop table if exists price_denorm_temp2;
create table if not exists price_denorm_temp1 as select marketname from price_denorm where id = "-";
create table if not exists price_denorm_temp2 as select marketname from price_denorm where id <> "-" and marketname in ( select marketname from price_denorm_temp1);
delete from price_denorm where id = "-" and marketname in ( select marketname from price_denorm_temp2) ;
delete from price_denorm_ld where id = "-" and marketname in ( select marketname from price_denorm_temp2) ;
delete from price_denorm_t1 where id = "-" and marketname in ( select marketname from price_denorm_temp2) ;
delete from price_denorm_t2 where id = "-" and marketname in ( select marketname from price_denorm_temp2) ;

DELETE FROM price_denorm_t1;

INSERT INTO price_denorm_t1(rank,id,symbol,name,twitter_screen_name,tweet_id,tweet_fetchTime,exchange,marketname,exchange_last_price,exchange_last_price_in,cmc_price_usd,cmc_price_btc,cmc_24h_volume_usd,cmc_market_cap_usd,cmc_percent_change_1h,cmc_percent_change_24h,cmc_percent_change_7d,is_new_market,created_at) 
SELECT
All_Coins.rank,
All_Coins.id,
All_Coins.symbol,
All_Coins.name,
All_Coins.twitter_screen_name,
All_Coins.tweet_id,
All_Coins.tweet_fetchTime,
All_Coins.exchange,
All_Coins.marketname,
All_Coins.exchange_last_price,
All_Coins.exchange_last_price_in,
All_Coins.price_usd AS cmc_price_usd,
All_Coins.price_btc AS cmc_price_btc,
All_Coins.24h_volume_usd AS cmc_24h_volume_usd,
All_Coins.market_cap_usd AS cmc_market_cap_usd,
All_Coins.percent_change_1h AS cmc_percent_change_1h,
All_Coins.percent_change_24h AS cmc_percent_change_24h,
All_Coins.percent_change_7d AS cmc_percent_change_7d,
All_Coins.is_new_market,
unix_timestamp() AS created_at
FROM
(SELECT
T1.rank,
T1.id,
T1.symbol,
T1.name,
T1.twitter_screen_name,
T1.tweet_id,
T1.tweet_fetchTime,
T1.exchange,
T1.marketname,
T1.exchange_last_price,
T1.exchange_last_price_in,
T1.price_usd,
T1.price_btc,
T1.24h_volume_usd,
T1.market_cap_usd,
T1.percent_change_1h,
T1.percent_change_24h,
T1.percent_change_7d,
CASE
  WHEN T1.exchange <> "coinmarketcap" AND P_DN.marketname IS NOT NULL AND P_DN.is_new_market <> "yes" THEN "no"
  WHEN P_DN.is_new_market = "yes" THEN "yes"
  WHEN T1.exchange <> "coinmarketcap" AND P_DN.marketname IS NULL     THEN "yes"
  ELSE NULL
END AS is_new_market
FROM
(SELECT 
CM.rank,
CM.id,
CM.symbol,
CM.name,
TW.twitter_screen_name AS twitter_screen_name,
TWEETS.tweet_id,
TWEETS.tweet_fetchTime,
CASE 
  WHEN COM.marketname="BTC-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="BTC-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="BTC-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="ETH-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="ETH-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="ETH-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="USDT-BTG" AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN "coinmarketcap"
  WHEN COM.marketname="btgusd"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN "coinmarketcap"
  WHEN COM.marketname="btgbtc"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN "coinmarketcap"
  WHEN (COM.marketname="BATBNB" OR COM.marketname="BATBTC" OR COM.marketname="BATETH")  AND CM.id="batcoin"       AND COM.exchange = "Binance"   THEN "coinmarketcap"
  WHEN (COM.marketname="BTGBTC" OR COM.marketname="BTGETH")                             AND CM.id="bitgem"        AND COM.exchange = "Binance"   THEN "coinmarketcap"
  WHEN (COM.marketname="CMTBNB" OR COM.marketname="CMTBTC" OR COM.marketname="CMTETH")  AND CM.id="comet"         AND COM.exchange = "Binance"   THEN "coinmarketcap"
  WHEN (COM.marketname="ICNBTC" OR COM.marketname="ICNETH")                             AND CM.id="icoin"         AND COM.exchange = "Binance"   THEN "coinmarketcap"
  WHEN (COM.marketname="ICXBNB" OR COM.marketname="ICXBTC" OR COM.marketname="ICXETH")  AND CM.id="icon-futures"  AND COM.exchange = "Binance"   THEN "coinmarketcap"
  WHEN (COM.marketname="KNCBTC" OR COM.marketname="KNCBTC")                             AND CM.id="kingn-coin"    AND COM.exchange = "Binance"   THEN "coinmarketcap"
  WHEN (COM.marketname="RCNBNB" OR COM.marketname="RCNBTC" OR COM.marketname="RCNETH")  AND CM.id="rcoin"         AND COM.exchange = "Binance"   THEN "coinmarketcap"
  ELSE COALESCE(COM.exchange,"coinmarketcap")
END AS exchange,
CASE 
  WHEN COM.marketname="BTC-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="BTC-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="BTC-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="ETH-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="ETH-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="ETH-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="USDT-BTG" AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN NULL
  WHEN COM.marketname="btgusd"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN NULL
  WHEN COM.marketname="btgbtc"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN NULL
  WHEN (COM.marketname="BATBNB" OR COM.marketname="BATBTC" OR COM.marketname="BATETH")  AND CM.id="batcoin"       AND COM.exchange = "Binance"   THEN NULL
  WHEN (COM.marketname="BTGBTC" OR COM.marketname="BTGETH")                             AND CM.id="bitgem"        AND COM.exchange = "Binance"   THEN NULL
  WHEN (COM.marketname="CMTBNB" OR COM.marketname="CMTBTC" OR COM.marketname="CMTETH")  AND CM.id="comet"         AND COM.exchange = "Binance"   THEN NULL
  WHEN (COM.marketname="ICNBTC" OR COM.marketname="ICNETH")                             AND CM.id="icoin"         AND COM.exchange = "Binance"   THEN NULL
  WHEN (COM.marketname="ICXBNB" OR COM.marketname="ICXBTC" OR COM.marketname="ICXETH")  AND CM.id="icon-futures"  AND COM.exchange = "Binance"   THEN NULL
  WHEN (COM.marketname="KNCBTC" OR COM.marketname="KNCBTC")                             AND CM.id="kingn-coin"    AND COM.exchange = "Binance"   THEN NULL
  WHEN (COM.marketname="RCNBNB" OR COM.marketname="RCNBTC" OR COM.marketname="RCNETH")  AND CM.id="rcoin"         AND COM.exchange = "Binance"   THEN NULL
  ELSE COM.marketname
END AS marketname, 
CASE 
  WHEN COM.marketname="BTC-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="BTC-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="BTC-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="ETH-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="ETH-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="ETH-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="USDT-BTG" AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN CM.exchange_last_price
  WHEN COM.marketname="btgusd"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN CM.exchange_last_price
  WHEN COM.marketname="btgbtc"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN CM.exchange_last_price
  WHEN (COM.marketname="BATBNB" OR COM.marketname="BATBTC" OR COM.marketname="BATETH")  AND CM.id="batcoin"       AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  WHEN (COM.marketname="BTGBTC" OR COM.marketname="BTGETH")                             AND CM.id="bitgem"        AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  WHEN (COM.marketname="CMTBNB" OR COM.marketname="CMTBTC" OR COM.marketname="CMTETH")  AND CM.id="comet"         AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  WHEN (COM.marketname="ICNBTC" OR COM.marketname="ICNETH")                             AND CM.id="icoin"         AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  WHEN (COM.marketname="ICXBNB" OR COM.marketname="ICXBTC" OR COM.marketname="ICXETH")  AND CM.id="icon-futures"  AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  WHEN (COM.marketname="KNCBTC" OR COM.marketname="KNCBTC")                             AND CM.id="kingn-coin"    AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  WHEN (COM.marketname="RCNBNB" OR COM.marketname="RCNBTC" OR COM.marketname="RCNETH")  AND CM.id="rcoin"         AND COM.exchange = "Binance"   THEN CM.exchange_last_price
  ELSE COALESCE(COM.exchange_last_price,CM.exchange_last_price)
END AS exchange_last_price,
CASE 
  WHEN COM.marketname="BTC-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="BTC-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="BTC-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="ETH-BAT"  AND CM.id="batcoin" AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="ETH-BTG"  AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="ETH-RCN"  AND CM.id="rcoin"   AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="USDT-BTG" AND CM.id="bitgem"  AND COM.exchange = "Bittrex"   THEN "btc"
  WHEN COM.marketname="btgusd"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN "btc"
  WHEN COM.marketname="btgbtc"   AND CM.id="bitgem"  AND COM.exchange = "Bitfinex"  THEN "btc"
  ELSE COALESCE(COM.exchange_last_price_in,"btc")
END AS exchange_last_price_in,
CM.price_usd,
CM.price_btc,
CM.24h_volume_usd,
CM.market_cap_usd,
CM.percent_change_1h,
CM.percent_change_24h,
CM.percent_change_7d
FROM 
(SELECT 
 rank,
 id,
 symbol,
 name,
 price_btc AS exchange_last_price,
 "btc" AS exchange_last_price_in,
 price_usd,
 price_btc,
 24h_volume_usd,
 market_cap_usd,
 percent_change_1h,
 percent_change_24h,
 percent_change_7d
 FROM coinmarketcap_dn
) CM
LEFT OUTER JOIN
twitterMapping TW
ON CM.id = TW.coinmarketcap_id
LEFT OUTER JOIN
( SELECT screen_name,tweet_id,fetchtime AS tweet_fetchTime 
  FROM tweets_dn
  WHERE inReplyToScreenName="" 
) TWEETS
ON TW.twitter_screen_name = TWEETS.screen_name
LEFT OUTER JOIN
(
SELECT BT_DN.marketname, BT_DN.coin, BT_DN.exchange, BT_DN.exchange_last_price, BT_DN.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'-',1)) AS coin,  
  "Bitbns" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in
  FROM bitbns_dn 
) BT_DN
UNION ALL
SELECT ZP_DN.marketname, ZP_DN.coin, ZP_DN.exchange, ZP_DN.exchange_last_price, ZP_DN.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'-',1)) AS coin,  
  "Zebpay" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in
  FROM zebpay_dn 
) ZP_DN
UNION ALL
SELECT KN_DN.marketname, KN_DN.coin, KN_DN.exchange, KN_DN.exchange_last_price, KN_DN.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'-',1)) AS coin,  
  "Koinex" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in
  FROM koinex_dn 
) KN_DN
UNION ALL
SELECT CD_DN.marketname, CD_DN.coin, CD_DN.exchange, CD_DN.exchange_last_price, CD_DN.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'-',1)) AS coin,  
  "Coindelta" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in
  FROM coindelta_dn 
) CD_DN
UNION ALL
SELECT WX_DN.marketname, WX_DN.coin, WX_DN.exchange, WX_DN.exchange_last_price, WX_DN.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'/',1)) AS coin,  
  "WazirX" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'/',-1)) AS exchange_last_price_in
  FROM wazirx_dn 
) WX_DN
UNION ALL
SELECT UC_DN.marketname, UC_DN.coin, UC_DN.exchange, UC_DN.exchange_last_price, UC_DN.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'-',1)) AS coin,  
  "Unocoin" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in
  FROM unocoin_dn 
) UC_DN
UNION ALL
SELECT HIT_DN.marketname, HIT_DN.coin, HIT_DN.exchange, HIT_DN.exchange_last_price, HIT_DN.exchange_last_price_in FROM
( SELECT 
  marketname,
  LEFT(replace(lower(marketname),'usdt','us-'), CHAR_LENGTH(replace(lower(marketname),'usdt','us-'))-3) AS coin,
  "Hitbtc" exchange,
  price AS exchange_last_price,
  replace(right(replace(lower(marketname),'usdt','us-'),3),'us-','usdt') AS exchange_last_price_in 
  FROM hitbtc_dn
) HIT_DN
UNION ALL
SELECT C_DN.marketname, C_DN.coin, C_DN.exchange, C_DN.exchange_last_price, C_DN.exchange_last_price_in FROM
( SELECT 
  marketname,
  lower(substring_index(marketname,'/',1)) coin,
  "Cryptopia" exchange,
  last_price AS exchange_last_price,
  lower(substring_index(marketname,'/',-1)) AS exchange_last_price_in
  FROM cryptopia_dn
) C_DN
UNION ALL
SELECT K_DN.marketname, K_DN.coin, K_DN.exchange, K_DN.exchange_last_price, K_DN.exchange_last_price_in FROM
( SELECT 
  marketname,
  lower(substring_index(marketname,'-',1)) coin,
  "Kucoin" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in
  FROM kucoin_dn
) K_DN
UNION ALL
SELECT IX.marketname, IX.coin, IX.exchange, IX.exchange_last_price, IX.exchange_last_price_in FROM
(SELECT marketname,
  lower(substring_index(marketname,'_',-1)) AS coin,  
  "Idex" exchange,
  price AS exchange_last_price,
  lower(substring_index(marketname,'_',1)) AS exchange_last_price_in
  FROM idex_dn 
) IX
UNION ALL
SELECT BX.marketname, BX.coin, BX.exchange, BX.exchange_last_price, BX.exchange_last_price_in FROM
(SELECT marketname,
  CASE 
   WHEN lower(substring_index(marketname,'-',-1)) = "bcc" THEN "bch" 
   WHEN lower(substring_index(marketname,'-',-1)) = "nbt" THEN "usnbt" 
   WHEN lower(substring_index(marketname,'-',-1)) = "iota" THEN "miota" 
   ELSE lower(substring_index(marketname,'-',-1)) 
  END coin, 
  "Bittrex" exchange,
  last AS exchange_last_price,
  lower(substring_index(marketname,'-',1)) AS exchange_last_price_in
  FROM bittrex_dn 
) BX
UNION ALL
SELECT BF.marketname, BF.coin, BF.exchange, BF.exchange_last_price, BF.exchange_last_price_in FROM
(SELECT marketname,
  CASE
   WHEN lower(substring(marketname,1,3)) = "dat" THEN "data"
   WHEN lower(substring(marketname,1,3)) = "dsh" THEN "dash"
   WHEN lower(substring(marketname,1,3)) = "iot" THEN "miota"
   WHEN lower(substring(marketname,1,3)) = "qsh" THEN "qash"
   WHEN lower(substring(marketname,1,3)) = "qtm" THEN "qtum"
   WHEN lower(substring(marketname,1,3)) = "yyw" THEN "yoyow"
   WHEN lower(substring(marketname,1,3)) = "aio" THEN "aion"
   WHEN lower(substring(marketname,1,3)) = "dad" THEN "dadi"
   WHEN lower(substring(marketname,1,3)) = "ios" THEN "iost"
   WHEN lower(substring(marketname,1,3)) = "mit" THEN "mith"
   WHEN lower(substring(marketname,1,3)) = "mna" THEN "mana"
   WHEN lower(substring(marketname,1,3)) = "poy" THEN "poly"
   WHEN lower(substring(marketname,1,3)) = "sng" THEN "sngls"
   WHEN lower(substring(marketname,1,3)) = "stj" THEN "storj"
   ELSE lower(substring(marketname,1,3)) 
  END coin, 
  "Bitfinex" exchange,
  last_price AS exchange_last_price,
  lower(substring(marketname,4,6)) AS exchange_last_price_in
  FROM bitfinex_dn
  WHERE lower(substring(marketname,1,3)) <> "rrt"
  AND lower(substring(marketname,1,3)) <> "bcu"  
) BF
UNION ALL
SELECT BN.marketname, 
CASE
 WHEN lower(BN.coin) = "iota" THEN "miota"
 WHEN lower(BN.coin) = "yoyo" THEN "yoyow"
 WHEN lower(BN.coin) = "bqx"  THEN "ethos"
 WHEN lower(BN.coin) = "ven"  THEN "vet"
 WHEN lower(BN.coin) = "bcc"  THEN "bch"
 ELSE lower(BN.coin)
END coin, 
BN.exchange, BN.exchange_last_price, BN.exchange_last_price_in FROM
(SELECT marketname,
  LEFT(replace(lower(marketname),'usdt','us-'), CHAR_LENGTH(replace(lower(marketname),'usdt','us-'))-3) AS coin,
  "Binance" exchange,
  price AS exchange_last_price,
  replace(right(replace(lower(marketname),'usdt','us-'),3),'us-','usdt') AS exchange_last_price_in 
  FROM binance_dn
) BN
WHERE BN.coin not in ( '','123')
) COM
ON lower(CM.symbol) = COM.coin) T1
LEFT OUTER JOIN
(SELECT marketname,is_new_market from price_denorm WHERE exchange IS NOT NULL GROUP BY marketname,is_new_market) P_DN
ON T1.marketname = P_DN.marketname
UNION ALL
SELECT 
rank,
id,
symbol,
name,
twitter_screen_name,
tweet_id,
tweet_fetchTime,
exchange,
marketname,
exchange_last_price,
exchange_last_price_in,
cmc_price_usd,
cmc_price_btc,
cmc_24h_volume_usd,
cmc_market_cap_usd,
cmc_percent_change_1h,
cmc_percent_change_24h,
cmc_percent_change_7d,
is_new_market
FROM price_denorm
WHERE id = "-"
) All_Coins
GROUP BY 
All_Coins.rank,
All_Coins.id,
All_Coins.symbol,
All_Coins.name,
All_Coins.twitter_screen_name,
All_Coins.tweet_id,
All_Coins.tweet_fetchTime,
All_Coins.exchange,
All_Coins.marketname,
All_Coins.exchange_last_price,
All_Coins.exchange_last_price_in,
All_Coins.price_usd,
All_Coins.price_btc,
All_Coins.24h_volume_usd,
All_Coins.market_cap_usd,
All_Coins.percent_change_1h,
All_Coins.percent_change_24h,
All_Coins.percent_change_7d,
All_Coins.is_new_market
;



DELETE FROM price_denorm_t2;

INSERT INTO price_denorm_t2(rank,id,symbol,name,exchange,marketname,exchange_last_price,exchange_last_price_in,is_new_market,created_at) 
SELECT
99999 AS rank,"-" AS id,"-" AS symbol,"-" AS name,MKTS.exchange,MKTS.marketname,MKTS.exchange_last_price,MKTS.exchange_last_price_in,"yes" AS is_new_market,unix_timestamp() AS created_at
FROM
(
SELECT "Bitbns" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in 
FROM bitbns_dn
UNION ALL
SELECT "Zebpay" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in 
FROM zebpay_dn
UNION ALL
SELECT "Koinex" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in 
FROM koinex_dn
UNION ALL
SELECT "Coindelta" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in 
FROM coindelta_dn
UNION ALL
SELECT "Wazirx" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'/',-1)) AS exchange_last_price_in 
FROM wazirx_dn
UNION ALL
SELECT "Unocoin" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'-',-1)) AS exchange_last_price_in 
FROM unocoin_dn
UNION ALL
SELECT 
"Hitbtc" AS exchange,marketname,price AS exchange_last_price,
replace(right(replace(lower(marketname),'usdt','us-'),3),'us-','usdt') AS exchange_last_price_in
FROM hitbtc_dn
UNION ALL
SELECT "Idex" AS exchange, marketname,price       AS exchange_last_price,lower(substring_index(marketname,'_',1)) AS exchange_last_price_in 
FROM idex_dn
UNION ALL
SELECT "Bittrex" AS exchange, marketname,last       AS exchange_last_price,lower(substring_index(marketname,'-',1)) AS exchange_last_price_in 
FROM bittrex_dn
UNION ALL
SELECT "Bitfinex" AS exchange,marketname,last_price AS exchange_last_price,lower(substring(marketname,4,6)) AS exchange_last_price_in 
FROM bitfinex_dn
WHERE   lower(substring(marketname,1,3)) <> "bcu"
AND     lower(substring(marketname,1,3)) <> "rrt"
UNION ALL
SELECT 
"Binance" AS exchange,marketname,price AS exchange_last_price,
replace(right(replace(lower(marketname),'usdt','us-'),3),'us-','usdt') AS exchange_last_price_in
FROM binance_dn
WHERE   marketname not in ("123456" , "ETC")
) MKTS
WHERE MKTS.marketname NOT IN ( SELECT marketname FROM price_denorm_t1 WHERE marketname IS NOT NULL GROUP BY marketname)
;



DELETE FROM price_denorm_ld;

INSERT INTO price_denorm_ld
(rank,id,symbol,name,twitter_screen_name,tweet_id,tweet_fetchTime,exchange,marketname,exchange_last_price,exchange_last_price_in,cmc_price_usd,cmc_price_btc,cmc_24h_volume_usd,cmc_market_cap_usd,cmc_percent_change_1h,cmc_percent_change_24h,cmc_percent_change_7d,is_new_market,created_at)
SELECT
COM.rank,COM.id,COM.symbol,COM.name,COM.twitter_screen_name,COM.tweet_id,COM.tweet_fetchTime,COM.exchange,COM.marketname,COM.exchange_last_price,COM.exchange_last_price_in,COM.cmc_price_usd,COM.cmc_price_btc,COM.cmc_24h_volume_usd,COM.cmc_market_cap_usd,COM.cmc_percent_change_1h,COM.cmc_percent_change_24h,COM.cmc_percent_change_7d,COM.is_new_market,COM.created_at
FROM
(
SELECT 
rank,id,symbol,name,twitter_screen_name,tweet_id,tweet_fetchTime,exchange,marketname,exchange_last_price,exchange_last_price_in,cmc_price_usd,cmc_price_btc,cmc_24h_volume_usd,cmc_market_cap_usd,cmc_percent_change_1h,cmc_percent_change_24h,cmc_percent_change_7d,is_new_market,created_at FROM price_denorm_t1
UNION ALL
SELECT 
rank,id,symbol,name,twitter_screen_name,tweet_id,tweet_fetchTime,exchange,marketname,exchange_last_price,exchange_last_price_in,cmc_price_usd,cmc_price_btc,cmc_24h_volume_usd,cmc_market_cap_usd,cmc_percent_change_1h,cmc_percent_change_24h,cmc_percent_change_7d,is_new_market,created_at FROM price_denorm_t2
UNION ALL
SELECT 
rank,id,symbol,name,twitter_screen_name,tweet_id,tweet_fetchTime,"Coinmarketcap" as exchange,NULL AS marketname,1.0 AS exchange_last_price,"btc" AS exchange_last_price_in,cmc_price_usd,cmc_price_btc,cmc_24h_volume_usd,cmc_market_cap_usd,cmc_percent_change_1h,cmc_percent_change_24h,cmc_percent_change_7d,"no" AS is_new_market,created_at FROM price_denorm_t1 WHERE lower(symbol) = "btc"
UNION ALL
SELECT 
rank, id, symbol, name, twitter_screen_name, tweet_id, tweet_fetchTime, "Coinmarketcap" as exchange, NULL AS marketname, cmc_price_btc AS exchange_last_price, "btc" AS exchange_last_price_in, cmc_price_usd, cmc_price_btc, cmc_24h_volume_usd, cmc_market_cap_usd, cmc_percent_change_1h, cmc_percent_change_24h, cmc_percent_change_7d, "no" AS is_new_market, created_at  FROM price_denorm_t1 WHERE lower(symbol) <> "btc"
UNION ALL
SELECT 
rank,id,symbol,name,twitter_screen_name,tweet_id,tweet_fetchTime,"Coinmarketcap" as exchange,NULL AS marketname,cmc_price_usd AS exchange_last_price,"usd" AS exchange_last_price_in,cmc_price_usd,cmc_price_btc,cmc_24h_volume_usd,cmc_market_cap_usd,cmc_percent_change_1h,cmc_percent_change_24h,cmc_percent_change_7d,"no" AS is_new_market,created_at FROM price_denorm_t1
)COM
GROUP BY 
COM.rank,
COM.id,
COM.symbol,
COM.name,
COM.twitter_screen_name,
COM.tweet_id,
COM.tweet_fetchTime,
COM.exchange,
COM.marketname,
COM.exchange_last_price,
COM.exchange_last_price_in,
COM.cmc_price_usd,
COM.cmc_price_btc,
COM.cmc_24h_volume_usd,
COM.cmc_market_cap_usd,
COM.cmc_percent_change_1h,
COM.cmc_percent_change_24h,
COM.cmc_percent_change_7d,
COM.is_new_market,
COM.created_at
;

DROP TABLE IF EXISTS price_denorm_new_exchange_check;
CREATE TABLE IF NOT EXISTS price_denorm_new_exchange_check AS
SELECT count(*) as counts FROM price_denorm_ld WHERE is_new_market="yes" AND exchange<>"coinmarketcap";

UPDATE price_denorm_ld P_DN  , price_denorm_new_exchange_check P_CHK
SET P_DN.is_new_market="no"
WHERE P_DN.is_new_market="yes" AND P_CHK.counts > 7
;


RENAME TABLE price_denorm TO price_denorm_md;
RENAME TABLE price_denorm_ld TO price_denorm;
RENAME TABLE price_denorm_md TO price_denorm_ld;
DELETE FROM price_denorm_ld ;
INSERT INTO price_denorm_ld SELECT * FROM price_denorm;


INSERT INTO alerts_subscription
(chatId,alert_type,alert_fetchTime,coin_symbol,is_first,alert_price,price_in)
SELECT
BM.chatId,
"p_watch",
unix_timestamp() AS alert_fetchTime,
"btc",
"yes",
0,
"usd"
FROM
( SELECT chatId FROM botMessages GROUP BY chatId ) BM
LEFT OUTER JOIN
(SELECT chatId FROM alerts_subscription_dn_ld WHERE alert_type = "p_watch" AND coin_symbol = "btc" GROUP BY chatId )AS_DN
ON BM.chatId = AS_DN.chatId
WHERE AS_DN.chatId is NULL;

INSERT INTO alerts_subscription_BKP SELECT * FROM alerts_subscription;

INSERT INTO alerts_subscription_dn_ld 
( id, chatId , alert_type, alert_fetchTime, coin_symbol, is_first, alert_price, price_in )
SELECT 
ALS.id, 
ALS.chatId , 
ALS.alert_type, 
ALS.alert_fetchTime, 
ALS.coin_symbol, 
ALS.is_first, 
CASE 
WHEN lower(ALS.price_in) = "usd" THEN COALESCE(CDN.price_usd,ALS.alert_price)
ELSE COALESCE(CDN.price_btc,ALS.alert_price)
END AS alert_price,
CASE
WHEN lower(ALS.price_in) = "usd" THEN "usd"
ELSE "btc" 
END AS price_in
FROM 
alerts_subscription ALS
LEFT OUTER JOIN
coinmarketcap_dn CDN
ON ALS.coin_symbol = CDN.symbol
AND ALS.alert_type = "p_watch"
;

DELETE FROM alerts_subscription WHERE (id,alert_fetchTime) IN ( SELECT id,alert_fetchTime from alerts_subscription_dn_ld group by id,alert_fetchTime);


INSERT INTO send_alerts
(
id,chatId,alert_type,new_alert_fetchTime,coin_symbol,is_first,alert_price,price_in,twitter_screen_name,tweet_id,coin_id,coin_name,exchange,new_price
)
SELECT 
COM.id,COM.chatId,COM.alert_type,COM.new_alert_fetchTime,COM.coin_symbol,COM.is_first,COM.alert_price,COM.price_in,COM.twitter_screen_name,COM.tweet_id,COM.coin_id,COM.coin_name,COM.exchange,COM.new_price
 FROM 
(
select 
AL.id,AL.chatId,AL.alert_type,P_DN.tweet_fetchTime AS new_alert_fetchTime,AL.coin_symbol,"no" AS is_first,0.0 AS alert_price,"btc" AS price_in,P_DN.twitter_screen_name,P_DN.tweet_id AS tweet_id,P_DN.id AS coin_id,P_DN.name AS coin_name,"Coinmarketcap" AS exchange,0.0 AS new_price
FROM alerts_subscription_dn_ld AL
,
price_denorm_ld P_DN
WHERE P_DN.tweet_fetchTime > AL.alert_fetchTime
AND   AL.alert_type = "tweet"
AND   AL.coin_symbol = P_DN.symbol
AND   P_DN.twitter_screen_name IS NOT NULL
UNION ALL
SELECT 
AL.id,
AL.chatId,
AL.alert_type,
P_DN.created_at AS new_alert_fetchTime,
AL.coin_symbol,
"no" AS is_first,
P_DN.cmc_last_price AS alert_price, 
AL.price_in,
P_DN.twitter_screen_name,
CASE 
WHEN (abs(P_DN.cmc_percent_change_1h) > 10 and (P_DN.created_at - AL.alert_fetchTime)> 3600) THEN P_DN.cmc_percent_change_1h 
ELSE P_DN.cmc_percent_change_24h 
END AS tweet_id,
P_DN.id AS coin_id,
P_DN.name AS coin_name,
"Coinmarketcap" AS exchange,
P_DN.cmc_last_price AS new_price
FROM alerts_subscription_dn_ld AL
,
(SELECT * FROM
(
SELECT 
created_at , cmc_price_usd AS cmc_last_price, twitter_screen_name , id , name , symbol, cmc_percent_change_1h, cmc_percent_change_24h, "usd" AS exchange_last_price_in
FROM
price_denorm
UNION ALL
SELECT 
created_at , cmc_price_btc AS cmc_last_price, twitter_screen_name , id , name , symbol, cmc_percent_change_1h, cmc_percent_change_24h, "btc" AS exchange_last_price_in
FROM
price_denorm
) T1
GROUP BY 
T1.created_at , T1.cmc_last_price , T1.twitter_screen_name , T1.id , T1.name , T1.symbol, T1.cmc_percent_change_1h, T1.cmc_percent_change_24h, T1.exchange_last_price_in
) P_DN
WHERE 
(
 (abs(P_DN.cmc_percent_change_1h)  > 10 and ((P_DN.created_at - AL.alert_fetchTime) > 3600  OR (abs(P_DN.cmc_last_price - AL.alert_price)*100)/AL.alert_price >= 10) 
 or 
 (abs(P_DN.cmc_percent_change_24h) > 10 and ((P_DN.created_at - AL.alert_fetchTime) > 86400 OR (abs(P_DN.cmc_last_price - AL.alert_price)*100)/AL.alert_price >= 10)
)
AND   AL.alert_type = "p_watch"
AND   AL.coin_symbol = P_DN.symbol
AND   lower(AL.price_in) = lower(P_DN.exchange_last_price_in)
UNION ALL
select AL.id,AL.chatId,AL.alert_type,P_DN.created_at AS new_alert_fetchTime,AL.coin_symbol,"no" AS is_first,AL.alert_price,AL.price_in,P_DN.twitter_screen_name,P_DN.tweet_id,P_DN.id as coin_id,P_DN.name as coin_name,CASE WHEN lower(AL.price_in)='usd' THEN "Coinmarketcap" ELSE P_DN.exchange END AS exchange,CASE WHEN lower(AL.price_in)='usd' THEN P_DN.cmc_price_usd ELSE P_DN.exchange_last_price END AS new_price
FROM alerts_subscription_dn_ld AL
,
price_denorm_ld P_DN
WHERE P_DN.exchange_last_price > AL.alert_price
AND   AL.alert_type = "p_incr"
AND   AL.coin_symbol = P_DN.symbol
AND   AL.is_first = "yes"
AND   lower(AL.price_in) = lower(P_DN.exchange_last_price_in)
UNION ALL
select AL.id,AL.chatId,AL.alert_type,P_DN.created_at AS new_alert_fetchTime,AL.coin_symbol,"no" AS is_first,AL.alert_price,AL.price_in,P_DN.twitter_screen_name,P_DN.tweet_id,P_DN.id as coin_id,P_DN.name as coin_name,CASE WHEN lower(AL.price_in)='usd' THEN "Coinmarketcap" ELSE P_DN.exchange END AS exchange,CASE WHEN lower(AL.price_in)='usd' THEN P_DN.cmc_price_usd ELSE P_DN.exchange_last_price END AS new_price
FROM alerts_subscription_dn_ld AL
,
price_denorm_ld P_DN
WHERE P_DN.exchange_last_price > AL.alert_price
AND   AL.alert_type = "p_incr"
AND   AL.coin_symbol = P_DN.symbol
AND   lower(AL.price_in) = lower(P_DN.exchange_last_price_in)
AND   (P_DN.created_at - AL.alert_fetchTime) > 21600
UNION ALL
select AL.id,AL.chatId,AL.alert_type,P_DN.created_at AS new_alert_fetchTime,AL.coin_symbol,"no" AS is_first,AL.alert_price,AL.price_in,P_DN.twitter_screen_name,P_DN.tweet_id,P_DN.id as coin_id,P_DN.name as coin_name,CASE WHEN lower(AL.price_in)='usd' THEN "Coinmarketcap" ELSE P_DN.exchange END AS exchange,CASE WHEN lower(AL.price_in)='usd' THEN P_DN.cmc_price_usd ELSE P_DN.exchange_last_price END AS new_price
FROM alerts_subscription_dn_ld AL
,
price_denorm_ld P_DN
WHERE P_DN.exchange_last_price < AL.alert_price
AND   AL.alert_type = "p_decr"
AND   AL.coin_symbol = P_DN.symbol
AND   AL.is_first = "yes"
AND   lower(AL.price_in) = lower(P_DN.exchange_last_price_in)
UNION ALL
select AL.id,AL.chatId,AL.alert_type,P_DN.created_at AS new_alert_fetchTime,AL.coin_symbol,"no" AS is_first,AL.alert_price,AL.price_in,P_DN.twitter_screen_name,P_DN.tweet_id,P_DN.id as coin_id,P_DN.name as coin_name,CASE WHEN lower(AL.price_in)='usd' THEN "Coinmarketcap" ELSE P_DN.exchange END AS exchange,CASE WHEN lower(AL.price_in)='usd' THEN P_DN.cmc_price_usd ELSE P_DN.exchange_last_price END AS new_price
FROM alerts_subscription_dn_ld AL
,
price_denorm_ld P_DN
WHERE P_DN.exchange_last_price < AL.alert_price
AND   AL.alert_type = "p_decr"
AND   AL.coin_symbol = P_DN.symbol
AND   lower(AL.price_in) = lower(P_DN.exchange_last_price_in)
AND   (P_DN.created_at - AL.alert_fetchTime) > 21600
UNION ALL
SELECT
AL.id,AL.chatId,AL.alert_type,P_DN.tweet_fetchTime AS new_alert_fetchTime,P_DN.symbol as coin_symbol,"no" AS is_first,0.0 AS alert_price,"btc" AS price_in,P_DN.twitter_screen_name,P_DN.tweet_id AS tweet_id,P_DN.id AS coin_id,P_DN.name AS coin_name,"Coinmarketcap" AS exchange,0.0 AS new_price
FROM 
(
SELECT "-1" id, "special_tweet" alert_type, BM.chatId , AS_DN.alert_fetchTime
FROM
( SELECT chatId FROM botMessages WHERE chatId > 0  GROUP BY chatId ) BM
,
( SELECT  max(alert_fetchTime) alert_fetchTime FROM alerts_subscription_dn_ld WHERE alert_type="special_tweet" ) AS_DN
)AL
,
(
SELECT
P_DN1.tweet_fetchTime,P_DN1.twitter_screen_name,P_DN1.tweet_id,P_DN1.id,P_DN1.name,P_DN1.symbol
FROM
price_denorm_ld P_DN1
JOIN
tweets_dn_ld TW_DN
ON P_DN1.tweet_id = TW_DN.tweet_id
WHERE lower(TW_DN.tweet) like '%fork%' OR lower(TW_DN.tweet) like '%rebranding%'
AND P_DN1.rank < 30
GROUP BY
P_DN1.tweet_fetchTime,P_DN1.twitter_screen_name,P_DN1.tweet_id,P_DN1.id,P_DN1.name,P_DN1.symbol
) P_DN
WHERE P_DN.tweet_fetchTime > AL.alert_fetchTime
AND   AL.alert_type = "special_tweet"
AND   P_DN.twitter_screen_name IS NOT NULL
) COM
GROUP BY
COM.id,COM.chatId,COM.alert_type,COM.new_alert_fetchTime,COM.coin_symbol,COM.is_first,COM.alert_price,COM.price_in,COM.twitter_screen_name,COM.tweet_id,COM.coin_id,COM.coin_name,COM.exchange,COM.new_price
;



insert into alerts_subscription_dn_ld (
id,chatId,alert_type,alert_fetchTime,coin_symbol,is_first,alert_price,price_in
)
select
id,chatId,alert_type,new_alert_fetchTime,coin_symbol,is_first,alert_price,price_in
FROM send_alerts
GROUP BY
id,chatId,alert_type,new_alert_fetchTime,coin_symbol,is_first,alert_price,price_in
;

DELETE FROM alerts_subscription_dn_ld_t1;
INSERT INTO alerts_subscription_dn_ld_t1 (
id,chatId,alert_type,alert_fetchTime,coin_symbol,is_first,alert_price,price_in
)
SELECT
id,chatId,alert_type,alert_fetchTime,coin_symbol,is_first,alert_price,price_in
FROM alerts_subscription_dn_ld
GROUP BY
id,chatId,alert_type,alert_fetchTime,coin_symbol,is_first,alert_price,price_in
;
DELETE FROM alerts_subscription_dn_ld;
INSERT INTO alerts_subscription_dn_ld SELECT * FROM alerts_subscription_dn_ld_t1;

delete from alerts_subscription_t1;
INSERT INTO alerts_subscription_t1 (id,alert_fetchTime)  select id , max(alert_fetchTime) as alert_fetchTime from alerts_subscription_dn_ld group by id;
delete from alerts_subscription_dn_ld where alert_fetchTime NOT IN ( select alert_fetchTime from alerts_subscription_t1 group by alert_fetchTime);
delete from alerts_subscription_dn_ld where (id,alert_fetchTime) NOT IN ( select id , alert_fetchTime from alerts_subscription_t1);

RENAME TABLE alerts_subscription_dn TO alerts_subscription_dn_md;
RENAME TABLE alerts_subscription_dn_ld TO alerts_subscription_dn;
RENAME TABLE alerts_subscription_dn_md TO alerts_subscription_dn_ld;
DELETE FROM alerts_subscription_dn_ld ;
INSERT INTO alerts_subscription_dn_ld SELECT * FROM alerts_subscription_dn;