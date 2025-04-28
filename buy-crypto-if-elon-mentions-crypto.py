import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

import aiohttp
import tweepy
import MetaTrader5 as mt5

# ─── CONFIG ────────────────────────────────────────────────────────────────────
TW_CONSUMER_KEY    = "consumer_key"
TW_CONSUMER_SECRET = "consumer_secret"
TW_ACCESS_TOKEN    = "access_key"
TW_ACCESS_SECRET   = "access_secret"
SENTIMENT_KEY      = "sentiment_key"
WATCHED_USER_ID    = "44196397"     # Elon Musk
CRYPTO             = "BTCUSD"
KEYWORDS           = {"bitcoin", "btc"}

POLL_INTERVAL      = 5              # seconds
EQUITY_SHARE       = 0.05           # 5% of equity per trade
MAGIC_NUMBER       = 66
# ────────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Precompile regex to strip non-alphanumerics
CLEANER = re.compile(r"[^A-Za-z0-9 ]+")

class TwitterWatcher:
    def __init__(self):
        auth = tweepy.OAuthHandler(TW_CONSUMER_KEY, TW_CONSUMER_SECRET)
        auth.set_access_token(TW_ACCESS_TOKEN, TW_ACCESS_SECRET)
        self.client = tweepy.API(auth, wait_on_rate_limit=True, retry_count=3)
        self.since_id: Optional[int] = None

    async def get_latest(self) -> Optional[str]:
        tweets = self.client.user_timeline(
            user_id=WATCHED_USER_ID,
            since_id=self.since_id,
            count=1,
            tweet_mode="extended"
        )
        if not tweets:
            return None
        tweet = tweets[0]
        self.since_id = tweet.id
        text = CLEANER.sub(" ", tweet.full_text)
        return text.lower()


class SentimentClient:
    ENDPOINT = "https://text-sentiment.p.rapidapi.com/analyze"

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def score(self, text: str) -> float:
        data = {"text": text}
        async with self.session.post(
            self.ENDPOINT,
            data=data,
            headers={
                "content-type": "application/x-www-form-urlencoded",
                "x-rapidapi-key": SENTIMENT_KEY,
                "x-rapidapi-host": "text-sentiment.p.rapidapi.com",
            },
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()
            return float(body.get("pos", 0))


class Trader:
    def __init__(self):
        if not mt5.initialize():
            raise RuntimeError(f"MT5 init failed: {mt5.last_error()}")
        logged = mt5.login(555)
        if not logged:
            raise RuntimeError(f"MT5 login failed: {mt5.last_error()}")

        info = mt5.account_info()
        if not info:
            raise RuntimeError("Could not fetch account info")
        self.equity = info.equity

    def can_trade(self, symbol: str) -> bool:
        pos = mt5.positions_get(symbol=symbol) or []
        orders = mt5.orders_get(symbol=symbol) or []
        return not pos and not orders

    def execute_buy(self, symbol: str):
        tick = mt5.symbol_info_tick(symbol)
        price = tick.bid
        lot   = round((self.equity * EQUITY_SHARE) / price, 2)

        sl = price * (1 - 0.05)
        tp = price * (1 + 0.10)

        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lot,
            "type": mt5.ORDER_TYPE_BUY,
            "price": price,
            "sl": sl,
            "tp": tp,
            "magic": MAGIC_NUMBER,
            "comment": "async-buy",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        res = mt5.order_send(req)
        if res.retcode == mt5.TRADE_RETCODE_DONE:
            logging.info(f"Bought {lot} {symbol} @ {price:.2f}, SL={sl:.2f}, TP={tp:.2f}")
        else:
            logging.error(f"Order failed: {res.retcode} {res.comment}")

async def main_loop():
    twitter = TwitterWatcher()
    trader = Trader()

    async with aiohttp.ClientSession() as http_sess:
        sentiment = SentimentClient(http_sess)

        logging.info("Starting monitor…")
        while True:
            try:
                text = await asyncio.to_thread(twitter.get_latest)
                if text and KEYWORDS.intersection(text.split()):
                    score = await sentiment.score(text)
                    logging.info(f"Tweet: {text[:50]}… Sentiment: {score:.2f}")
                    if score > 0 and trader.can_trade(CRYPTO):
                        trader.execute_buy(CRYPTO)
                    else:
                        logging.info("No trade: sentiment<=0 or existing position.")
                await asyncio.sleep(POLL_INTERVAL)

            except Exception as e:
                logging.exception("Error in main loop, backing off 5s:")
                await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main_loop())
