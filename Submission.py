# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.

import asyncio
import itertools

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side

POSITION_LIMIT = 95
TICK_SIZE_IN_CENTS = 100

# Tune-able parameters
LOT_SIZE = 50
SOFT_MAX_ORDERS = 60
SPREAD_FACTOR = 0.7
MIN_ORDER_PRIORITY = 0.25
STABILITY_CONSTANT = 0.3

"""
FAIR VALUE:
    Calculated using a basic inverse volume weighted average:
    1). First we calculate the (volume weighted) average bid and ask prices using about 50% of the volume.
    2). Next we take the inverse volume weighted average of these two values
    
SPREAD SIZE:
    Currently, spread size is given as SPREAD_FACTOR * the distance between the two average bid/ask prices described in Step 1).
    
MAKE MARKET:
    Called everytime the orderbook is updated.
    Our ideal bid-ask position is to own zero stocks and zero futures. Our ideal market has SOFT_MAX_ORDERS bids/ asks at leading_bid/ask_price.
    
    So, to make our market, we first calculate our ideal spread and prices. 
    Next we exit out of any orders that deviate too far from these ideal prices.
    We then enter a certain quantity of bid/ asks. This quantity will be the minimum of LOT_SIZE, the difference
    between the current number of orders and the SOFT_MAX_ORDERS, and the difference between the POSITION_LIMIT and
    our combined position and orders.
    Further, in order to encourage the program to hold a neutral position, we decrease the the SOFT_MAX_ORDERS for 
    each side (bid/ask) to encourage the trader to tend towards a neutral position.

    
ON ORDERBOOK UPDATE:
    Call make_market() function
    
ON ORDER FILLED:
    When an order is filled, we immediately submit a hedge order so as to bring our total exposure to zero. 
    Also, we re-enter this order at one tick removed from the filled price. This could cause large sudden losses if the,
    price was to suddenly rise/ fall. So we limit the number of times we can consecutively re-enter an order in a particular direction.

 
  """

class InternalOrder:
    """An object which stores an active order"""

    def __init__(self, order_id, side, volume, price):
        self.order_id = order_id
        self.side = side
        self.volume = volume
        self.price = price


class OrderList:
    def __init__(self, side):
        self.__orders = []
        self.__order_ids = []
        self.__total_volume = 0
        self.__side = side

    def get_order_ids(self):
        return self.__order_ids

    def create_order(self, order_id, side, volume, price):
        self.__orders.append(InternalOrder(order_id, side, volume, price))
        self.__order_ids.append(order_id)
        self.__total_volume += volume

    def cancel_id(self, order_id):
        to_remove = self.get_order(order_id)
        self.__orders.remove(to_remove)
        self.__order_ids.remove(order_id)

        self.__total_volume -= to_remove.volume

    def get_order(self, order_id):
        for order in self.__orders:
            if order.order_id == order_id:
                return order

        print("Error: Trying to fetch order which doesn't exist")

    def update_volume(self, order_id, filled_volume):
        order = self.get_order(order_id)
        order.volume -= filled_volume

        if order.volume == 0:
            self.cancel_id(order_id)

        self.__total_volume -= filled_volume

    def best_price(self):
        prices = [order.price for order in self.__orders]

        if not prices:
            return 0

        if self.__side == Side.BID:
            return max(prices)
        elif self.__side == Side.ASK:
            return min(prices)

    def total_volume(self):
        return self.__total_volume


class Market:
    def __init__(self):
        self.__asks = OrderList(Side.ASK)
        self.__bids = OrderList(Side.BID)

        self.__position = 0

    def num_order(self, side):
        if side == Side.BID:
            return self.__bids.total_volume()
        elif side == Side.ASK:
            return self.__asks.total_volume()

    def create_order(self, order_id, side, volume, price):
        if side == Side.BID:
            self.__bids.create_order(order_id, side, volume, price)
        elif side == Side.ASK:
            self.__asks.create_order(order_id, side, volume, price)

    def cancel_order(self, order_id):
        if order_id in self.__bids.get_order_ids():
            self.__bids.cancel_id(order_id)
        elif order_id in self.__asks.get_order_ids():
            self.__asks.cancel_id(order_id)

    def order_filled(self, order_id, volume_filled):
        if order_id in self.__bids.get_order_ids():
            self.__bids.update_volume(order_id, volume_filled)
            self.__position += volume_filled
        elif order_id in self.__asks.get_order_ids():
            self.__asks.update_volume(order_id, volume_filled)
            self.__position -= volume_filled

    def get_position(self):
        return self.__position

    def has_pos(self, side):
        if side == Side.BID:
            return bool(self.__bids.get_order_ids())
        elif side == Side.ASK:
            return bool(self.__asks.get_order_ids())

    def best_price(self, side):
        if side == Side.BID:
            return self.__bids.best_price()
        elif side == Side.ASK:
            return self.__asks.best_price()

    def get_temp_id(self, side):
        if side == Side.BID:
            order_ids = self.__bids.get_order_ids()
            return 0 if not order_ids else order_ids[0]
        elif side == Side.ASK:
            order_ids = self.__asks.get_order_ids()
            return 0 if not order_ids else order_ids[0]

    def get_id_list(self, side):
        if side == Side.BID:
            return self.__bids.get_order_ids()
        elif side == Side.ASK:
            return self.__asks.get_order_ids()

    def get_order(self, order_id):
        if order_id in self.get_id_list(Side.BID):
            return self.__bids.get_order(order_id)
        elif order_id in self.get_id_list(Side.ASK):
            return self.__asks.get_order(order_id)


def round_to_tick_size(x):
    return round(x // TICK_SIZE_IN_CENTS) * TICK_SIZE_IN_CENTS


def weighted_average(arr, weights):
    # returns the average of the arr, weighted by the weights.
    if sum(weights) == 0:
        return 0
    return sum([x * w for x, w in zip(arr, weights)]) / sum(weights)


class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)

        # create an order list for bids/asks in each exchange
        self.ETFMarket = Market()
        self.FUTUREMarket = Market()

        # keep track of the last type of order that was filled in order to prevent 'spiralling' due to a sudden
        # price increase/ decrease
        self.last_order_filled = Side.BID

        # or testing
        self.position_history = []

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())

        # if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
        #     self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled, partially or fully.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.

        If the order was unsuccessful, both the price and volume will be zero.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

        self.FUTUREMarket.order_filled(client_order_id, volume_filled=volume)

    def fair_value(self, ask_prices, bid_prices, ask_volumes, bid_volumes):
        """
        Try taking a volume weighted average. We consider the top X% of bid/ ask order respectively for this.

        We assume that someone selling a stock, is an indication it's value should be lower than it is.
        Conversely, someone bidding for a stock, is an indication it's value should be higher than it is.

        See 7th March 22:32; REF A. For explanation of this value strategy
        """
        relevant_volume_proportion = 0.5

        # determine how many bid and offer prices we need to have considered 50% of the volume
        total_bid_volume = sum(bid_volumes)
        total_ask_volume = sum(ask_volumes)

        # start with bids. r stands for relevant
        r_bid_prices = []
        r_bid_volumes = []
        r_bid_volume = 0
        for price, volume in zip(bid_prices, bid_volumes):
            r_bid_prices.append(price)
            r_bid_volumes.append(volume)
            r_bid_volume += volume

            if r_bid_volume >= total_bid_volume * relevant_volume_proportion:
                break

        # now do asks. r stands for relevant, once again
        r_ask_prices = []
        r_ask_volumes = []
        r_ask_volume = 0
        for price, volume in zip(ask_prices, ask_volumes):
            r_ask_prices.append(price)
            r_ask_volumes.append(volume)
            r_ask_volume += volume

            if r_ask_volume >= total_ask_volume * relevant_volume_proportion:
                break

        # Now take the weighted average within the bid and ask prices separately
        avg_bid_price = weighted_average(r_bid_prices, r_bid_volumes)
        avg_ask_price = weighted_average(r_ask_prices, r_ask_volumes)

        # Now take a weighted average of these two, by the others volume
        fair_value = weighted_average([avg_bid_price, avg_ask_price], [r_ask_volume, r_bid_volume])

        self.logger.info(f"Fair Value: {fair_value}")

        # Now finally calculate the spread based of the avg_bid and avg_ask prices
        spread = (avg_ask_price - avg_bid_price) * SPREAD_FACTOR

        return fair_value, spread

    def send_etf_order(self, side, volume, price):
        # validate we are within position limits
        if (side == Side.BID and self.ETFMarket.get_position() + self.ETFMarket.num_order(Side.BID) + volume > POSITION_LIMIT) or \
                (side == Side.ASK and self.ETFMarket.get_position() - self.ETFMarket.num_order(Side.ASK) - volume < -POSITION_LIMIT):
            self.logger.error("Order submitted which would exceed position limit.")
            return

        order_id = next(self.order_ids)

        # finally send the order to the exchange
        self.send_insert_order(order_id, side, price, volume, Lifespan.GOOD_FOR_DAY)

        # update our internal order book
        self.ETFMarket.create_order(order_id, side, volume, price)

    def cancel_etf_order(self, order_id):
        self.send_cancel_order(order_id)

    def hedge_trade(self, price):
        # Hedge our position.
        # When we are hedging, we will never be able to hedge perfectly, so we have a margin for error
        hedge_margin = +TICK_SIZE_IN_CENTS * 1

        futures_volume = - (self.ETFMarket.get_position() + (self.FUTUREMarket.get_position() +
                            self.FUTUREMarket.num_order(Side.BID) - self.FUTUREMarket.num_order(Side.ASK)))

        # print(f"'\nStocks owned: {self.ETFMarket.get_position()}")
        # print(f"'Futures owned: {self.FUTUREMarket.get_position()}")
        # print(f"'Futures bidded: {self.FUTUREMarket.num_order(Side.BID)}")
        # print(f"'Futures asked: {self.FUTUREMarket.num_order(Side.ASK)}")
        # print(f"'Proposed volume: {futures_volume}")

        if futures_volume > 0 and self.FUTUREMarket.get_position() + futures_volume < POSITION_LIMIT:
            # if volume > 0, we are buying futures

            # send the hedge order internally and externally
            order_id = next(self.order_ids)
            self.send_hedge_order(order_id, Side.BID, price + hedge_margin, futures_volume)
            self.FUTUREMarket.create_order(order_id, Side.BUY, futures_volume, price)

        elif futures_volume < 0 and self.FUTUREMarket.get_position() + futures_volume > -POSITION_LIMIT:
            # if volume < 0, we are selling futures
            futures_volume = -futures_volume

            # send the hedge order internally and externally
            order_id = next(self.order_ids)
            self.send_hedge_order(order_id, Side.ASK, price - hedge_margin, futures_volume)
            self.FUTUREMarket.create_order(order_id, Side.SELL, futures_volume, price)

    def make_market(self, fair_value, spread_size, ask_prices, bid_prices, ask_volumes, bid_volumes):
        """
        NOTES: Consider the benefit of high spread and high volume, vs low spread and low volume.
        https://www.informs-sim.org/wsc15papers/027.pdf
        """

        # determine new bid and ask prices
        leading_bid_price = fair_value - (spread_size / 2)
        leading_ask_price = fair_value + (spread_size / 2)

        # round to tick size
        leading_bid_price = round_to_tick_size(leading_bid_price)
        leading_ask_price = round_to_tick_size(leading_ask_price)

        # if the order book is stable, we want our bid/ask prices to be within the top MIN_ORDER_PRIORITY% priority
        # total_bids = sum(bid_volumes)
        # total_asks = sum(ask_volumes)
        # if abs((total_bids - total_asks) / (total_bids + total_asks + 1)) < STABILITY_CONSTANT:
        #     min_bid_price = bid_prices[0]
        #     bids_counted = 0
        #     for price, vol in zip(bid_prices, bid_volumes):
        #         bids_counted += vol
        #         if bids_counted > total_bids * MIN_ORDER_PRIORITY:
        #             break
        #         min_bid_price = bid_prices[0]
        #
        #     max_ask_price = ask_prices[0]
        #     asks_counted = 0
        #     for price, vol in zip(ask_prices, ask_volumes):
        #         asks_counted += vol
        #         if asks_counted > total_asks * MIN_ORDER_PRIORITY:
        #             break
        #         max_ask_price = bid_prices[0]
        #
        #     leading_bid_price = max(leading_bid_price, min_bid_price)
        #     leading_ask_price = min(leading_ask_price, max_ask_price)

        if leading_bid_price <= MINIMUM_BID or leading_ask_price >= MAXIMUM_ASK:
            self.logger.warning("BID/ASK Price exceeded limit")
            return

        """
        We will go through each bid and ask we currently have. 
        We will only exit out of a position if it is a certain distance away from our ideal spread.
        """
        # cancel orders if they deviate too far from our perfect spread
        for bid_id in self.ETFMarket.get_id_list(Side.BID):
            order = self.ETFMarket.get_order(bid_id)

            if (order.price >= fair_value - spread_size / 4 or
                    order.price <= fair_value - 2 * spread_size / 4):
                self.cancel_etf_order(bid_id)
        # now buy at most a 'lot' of shares, without exceeding position limit.
        for ask_id in self.ETFMarket.get_id_list(Side.ASK):
            order = self.ETFMarket.get_order(ask_id)

            if (order.price <= fair_value + spread_size / 4 or
                    order.price >= fair_value + 2 * spread_size / 4):
                self.cancel_etf_order(ask_id)

        """
        Ideally, we want to own zero stocks and zero futures. So we will modify the volume of our orders so as to reset
        our position to zero stocks and zero futures.
        """
        # minus one because of weird hedging bug at position limit
        volume = min(LOT_SIZE,
                     POSITION_LIMIT - (self.ETFMarket.num_order(Side.BID) + self.ETFMarket.get_position()),
                     SOFT_MAX_ORDERS * (100 - self.ETFMarket.get_position()) // 100 - self.ETFMarket.num_order(Side.BID)
                     ) - 1
        if volume > 0:
            self.send_etf_order(Side.BID, volume, leading_bid_price)

        # minus one because of weird hedging bug at position limit
        volume = min(LOT_SIZE,
                     POSITION_LIMIT + (-self.ETFMarket.num_order(Side.ASK) + self.ETFMarket.get_position()),
                     SOFT_MAX_ORDERS * (100 + self.ETFMarket.get_position()) // 100 - self.ETFMarket.num_order(Side.ASK)
                     ) - 1
        if volume > 0:
            self.send_etf_order(Side.ASK, volume, leading_ask_price)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        # if we have received price data for the ETF
        if instrument == Instrument.ETF:
            fair_value, spread_size = self.fair_value(ask_prices, bid_prices, ask_volumes, bid_volumes)

            self.make_market(fair_value, spread_size, ask_prices, bid_prices, ask_volumes, bid_volumes)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)
        # update our internal order book
        self.ETFMarket.order_filled(order_id=client_order_id, volume_filled=volume)

        """
        If an order has just been filled, we want to re-enter the order, at one tick removed.
        Except, to limit the losses due to a sudden price change, we limit the amount of times we can consecutively 
        re-enter an order in a particular direction.
        """
        if client_order_id in self.ETFMarket.get_id_list(Side.ASK):
            if self.last_order_filled == Side.BID:
                self.send_etf_order(Side.ASK, volume, price + TICK_SIZE_IN_CENTS)

            self.last_order_filled = Side.ASK

        if client_order_id in self.ETFMarket.get_id_list(Side.BID):
            if self.last_order_filled == Side.ASK:
                self.send_etf_order(Side.BID, volume, price - TICK_SIZE_IN_CENTS)

            self.last_order_filled = Side.BID

        # hedge this trade
        self.hedge_trade(price)

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)

        # It could be the case that an order is cancelled, but filled before that cancel goes through.
        # So we cancel orders in the on_order_filled function, and also here.
        if remaining_volume == 0:
            self.ETFMarket.cancel_order(client_order_id)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.

        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.

        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)
