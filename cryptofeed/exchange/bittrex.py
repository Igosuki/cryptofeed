'''
Copyright (C) 2017-2019  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from uuid import uuid4
import json
import logging
from decimal import Decimal
from threading import Thread
from queue import Queue
from sortedcontainers import SortedDict as sd

from cryptofeed.feed import SignalRFeed
from cryptofeed.defines import BUY, SELL, BID, ASK, TRADES, L2_BOOK, L3_BOOK, BITTREX
from cryptofeed.standards import pair_exchange_to_std, feed_to_exchange, timestamp_normalize
from websockets import ConnectionClosed
from websockets.exceptions import InvalidStatusCode
from signalr_aio import Connection
from cryptofeed.signalr.events import *

try:
    from cfscrape import create_scraper as Session
except ImportError:
    from requests import Session

LOG = logging.getLogger('feedhandler')

class BittrexConnection(object):
    def __init__(self, conn, hub):
        self.conn = conn
        self.corehub = hub
        self.id = uuid4().hex

class BittrexMethods:
    SUBSCRIBE_TO_EXCHANGE_DELTAS = 'SubscribeToExchangeDeltas'
    SUBSCRIBE_TO_SUMMARY_DELTAS = 'SubscribeToSummaryDeltas'
    SUBSCRIBE_TO_SUMMARY_LITE_DELTAS = 'SubscribeToSummaryLiteDeltas'
    QUERY_SUMMARY_STATE = 'QuerySummaryState'
    QUERY_EXCHANGE_STATE = 'QueryExchangeState'
    GET_AUTH_CONTENT = 'GetAuthContext'
    AUTHENTICATE = 'Authenticate'

class BittrexParameters:
    # Connection parameters
    URL = 'https://socket.bittrex.com/signalr'
    HUB = 'c2'
    # Callbacks
    MARKET_DELTA = 'uE'
    SUMMARY_DELTA = 'uS'
    SUMMARY_DELTA_LITE = 'uL'
    BALANCE_DELTA = 'uB'
    ORDER_DELTA = 'uO'

class ErrorMessages:
    INVALID_TICKER_INPUT = 'Tickers must be submitted as a list.'

class OtherConstants:
    CF_SESSION_TYPE = '<class \'cfscrape.CloudflareScraper\'>'



class Bittrex(SignalRFeed):
    id = BITTREX
    # API documentation: https://www.bitstamp.net/websocket/v2/

    def __init__(self, pairs=None, channels=None, callbacks=None, **kwargs):
        self.url = BittrexParameters.URL
        self.threads = []
        self._handle_connect()
        super().__init__(
            '',
            pairs=pairs,
            channels=channels,
            callbacks=callbacks,
            **kwargs
        )

    def _start_main_thread(self):
        self.control_queue = Queue()
        self.control_queue.put(ConnectEvent())
        thread = Thread(target=self.control_queue_handler, daemon=True, name='ControlQueueThread')
        self.threads.append(thread)
        thread.start()

    def _handle_connect(self):
        connection = Connection(self.url, Session())
        hub = connection.register_hub(BittrexParameters.HUB)
        connection.received += self._on_debug
        connection.error += self.on_error
        hub.client.on(BittrexParameters.MARKET_DELTA, self._on_public)
        hub.client.on(BittrexParameters.SUMMARY_DELTA, self._on_public)
        hub.client.on(BittrexParameters.SUMMARY_DELTA_LITE, self._on_public)
        hub.client.on(BittrexParameters.BALANCE_DELTA, self._on_private)
        hub.client.on(BittrexParameters.ORDER_DELTA, self._on_private)
        self.connection = BittrexConnection(connection, hub)
        thread = Thread(target=self._connection_handler, daemon=True, name='SocketConnectionThread')
        self.threads.append(thread)
        thread.start()

    def _connection_handler(self):
        if str(type(self.connection.conn.session)) == OtherConstants.CF_SESSION_TYPE:
            LOG.info('Establishing connection to Bittrex through {}.'.format(self.url))
            LOG.info('cfscrape detected, using a cfscrape session instead of requests.')
        else:
            LOG.info('Establishing connection to Bittrex through {}.'.format(self.url))
        try:
            self.connection.conn.start()
        except ConnectionClosed as e:
            if e.code == 1000:
                LOG.info('Bittrex connection successfully closed.')
            elif e.code == 1006:
                event = ReconnectEvent(e.args[0])
                self.control_queue.put(event)
        except ConnectionError as e:
            raise ConnectionError(e)
        except InvalidStatusCode as e:
            message = "Status code not 101: {}".format(e.status_code)
            event = ReconnectEvent(message)
            self.control_queue.put(event)

    async def _is_query_invoke(self, kwargs):
        if 'R' in kwargs and type(kwargs['R']) is not bool:
            invoke = self.invokes[int(kwargs['I'])]['invoke']
            if invoke == BittrexMethods.GET_AUTH_CONTENT:
                signature = await create_signature(self.credentials['api_secret'], kwargs['R'])
                event = SubscribeEvent(BittrexMethods.AUTHENTICATE, self.credentials['api_key'], signature)
                self.control_queue.put(event)
            else:
                msg = await process_message(kwargs['R'])
                if msg is not None:
                    msg['invoke_type'] = invoke
                    msg['ticker'] = self.invokes[int(kwargs['I'])].get('ticker')
                    await self.on_public(msg)

    def control_queue_handler(self):
        while True:
            event = self.control_queue.get()
            if event is not None:
                if event.type == EventTypes.CONNECT:
                    self._handle_connect()
                elif event.type == EventTypes.SUBSCRIBE:
                    self._handle_subscribe(event.invoke, event.payload)
                elif event.type == EventTypes.RECONNECT:
                    self._handle_reconnect(event.error_message)
                elif event.type == EventTypes.CLOSE:
                    self.connection.conn.close()
                    break
                self.control_queue.task_done()

    def _handle_subscribe(self, invoke, payload):
        if invoke in [BittrexMethods.SUBSCRIBE_TO_EXCHANGE_DELTAS, BittrexMethods.QUERY_EXCHANGE_STATE]:
            for ticker in payload[0]:
                self.invokes.append({'invoke': invoke, 'ticker': ticker})
                self.connection.corehub.server.invoke(invoke, ticker)
                LOG.info('Successfully subscribed to [{}] for [{}].'.format(invoke, ticker))
        elif invoke == BittrexMethods.GET_AUTH_CONTENT:
            self.connection.corehub.server.invoke(invoke, payload[0])
            self.invokes.append({'invoke': invoke, 'ticker': payload[0]})
            LOG.info('Retrieving authentication challenge.')
        elif invoke == BittrexMethods.AUTHENTICATE:
            self.connection.corehub.server.invoke(invoke, payload[0], payload[1])
            LOG.info('Challenge retrieved. Sending authentication. Awaiting messages...')
            # No need to append invoke list, because AUTHENTICATE is called from successful GET_AUTH_CONTENT.
        else:
            self.invokes.append({'invoke': invoke, 'ticker': None})
            self.connection.corehub.server.invoke(invoke)
            LOG.info('Successfully invoked [{}].'.format(invoke))

    async def _on_debug(self, **kwargs):
        # `QueryExchangeState`, `QuerySummaryState` and `GetAuthContext` are received in the debug channel.
        await self._is_query_invoke(kwargs)

    # =======================
    # Private Channel Methods
    # =======================

    async def _on_public(self, args):
        msg = await process_message(args[0])
        if 'D' in msg:
            if len(msg['D'][0]) > 3:
                msg['invoke_type'] = BittrexMethods.SUBSCRIBE_TO_SUMMARY_DELTAS
            else:
                msg['invoke_type'] = BittrexMethods.SUBSCRIBE_TO_SUMMARY_LITE_DELTAS
        else:
            msg['invoke_type'] = BittrexMethods.SUBSCRIBE_TO_EXCHANGE_DELTAS
        await self.on_public(msg)

    async def _on_private(self, args):
        msg = await process_message(args[0])
        await self.on_private(msg)

    # ======================
    # Public Channel Methods
    # ======================

    async def on_public(self, msg):
        pass

    async def on_private(self, msg):
        pass

    async def on_error(self, args):
        LOG.error(args)

    async def message_handler(self, msg: str, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)
        print(msg)

    async def subscribe(self, websocket):
        for channel in self.channels if not self.config else self.config:
            for pair in self.pairs if not self.config else self.config[channel]:
                await websocket.send(
                    json.dumps({
                        "event": "bts:subscribe",
                        "data": {
                            "channel": "{}_{}".format(channel, pair)
                        }
                    }))

