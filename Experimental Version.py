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
from ready_trader_go.limiter import FrequencyLimiter
import time
import queue

# constants
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS


class Order:
    # stores an order internally

    def __init__(self, order_id, side, volume, price):
        self.order_id = order_id
        self.side = side
        self.volume = volume
        self.original_volume = volume
        self.price = price
        self.sequence_number = SequenceNumber


class OrderList:
    # keeps track of a list of either bids/asks
    def __init__(self, side):
        self.__orders = []  # array of Orders
        self.__order_ids = []  # array of order ids
        self.__total_volume = 0  # total volume of orders
        self.__side = side  # side eg. bid/ask

        # keep track of the total orders posted/ filled for pre-hedging
        self.__total_orders_posted = 0
        self.__total_orders_filled = 0

    def get_order_ids(self):
        return self.__order_ids

    def create_order(self, order_id, side, volume, price):
        self.__orders.append(Order(order_id, side, volume, price))
        self.__order_ids.append(order_id)
        self.__total_volume += volume

        self.__total_orders_posted += volume

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

        return_value = order.volume  # return the current order volume

        if order.volume == 0:
            self.cancel_id(order_id)

        self.__total_volume -= filled_volume
        self.__total_orders_filled += filled_volume

        return return_value

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

    def get_avg_fill_rate(self):
        # average rate at which our posted orders are filled
        if self.__total_orders_posted == 0:
            return 0

        return self.__total_orders_filled / self.__total_orders_posted


class Market:
    # We have one market class for stocks, and one for futures
    def __init__(self, market_type):
        self.__asks = OrderList(Side.ASK)
        self.__bids = OrderList(Side.BID)

        self.__position = 0

        self.__realised_profit = 0  # this is the profit made from pairs of bought/ sold stocks
        self.__unrealised_bids = []
        self.__unrealised_asks = []

        self.__market_type = market_type  # whether this is an ETF or futures

        self.__filled_order_side = []  # previously filled orders. we store it to detect prolonged market moves

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

    def order_filled(self, order_id, volume_filled, price):
        # we need price as an input because hedges are purchased at any price

        if order_id in self.__bids.get_order_ids():
            # store this unrealised profit
            self.__unrealised_bids += [price] * volume_filled

            order_remains = self.__bids.update_volume(order_id, volume_filled)
            if not order_remains:
                self.__filled_order_side += [Side.BID]

            self.__position += volume_filled

        elif order_id in self.__asks.get_order_ids():
            # store this unrealised profit
            self.__unrealised_asks += [price] * volume_filled

            order_remains = self.__asks.update_volume(order_id, volume_filled)
            if not order_remains:
                self.__filled_order_side += [Side.ASK]
            self.__position -= volume_filled

        # update realised profit
        self.update_realised_profit()

    def update_realised_profit(self):
        num_netting_trades = min(len(self.__unrealised_bids), len(self.__unrealised_asks))
        if num_netting_trades == 0:
            return

        # calculate the profit from the netted trades
        for index in range(num_netting_trades):
            bid_price = self.__unrealised_bids.pop(0)
            ask_price = self.__unrealised_asks.pop(0)

            self.__realised_profit += ask_price - bid_price

        OverallStats.realised_profit_history += [[SequenceNumber, self.__realised_profit]]

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

    def contains_id(self, id_in):
        return (id_in in self.get_id_list(Side.BID)) or (id_in in self.get_id_list(Side.ASK))

    def potential_positive_exposure(self):
        # potential exposure is the largest amount to which we'd be exposed our orders were filled
        return self.__position + self.num_order(Side.BID)

    def potential_negative_exposure(self):
        # potential exposure is the largest amount to which we'd be exposed our orders were filled
        return self.__position - self.num_order(Side.ASK)

    def get_realised_profit(self):
        return self.__realised_profit

    def get_avg_fill_rate(self, side):
        if side == Side.BID:
            return self.__bids.get_avg_fill_rate()
        else:
            return self.__asks.get_avg_fill_rate()

    def detect_market_move(self):
        if len(self.__filled_order_side) < TraderParameters.MAX_ORDERS_IN_ROW:
            return -5

        most_recent_order_side = self.__filled_order_side[-1]
        for recent_order_side in self.__filled_order_side[-TraderParameters.MAX_ORDERS_IN_ROW:]:
            if recent_order_side != most_recent_order_side:
                return -5  # arbitary number which isn't Side.BID or Side.ASK

        return most_recent_order_side


class Statistics:
    def __init__(self):
        # checks that we aren't making too many calls
        self.total_limiter_calls = 0
        self.total_limiter_cutoffs = 0

        # stores our realised profit
        self.realised_profit_history = []

        # init the log file
        self.__log_file_name = "custom_log.csv"
        self.__log_file_columns = ["Sequence number", "ETF realised profit", "Futures realised profit",
                                   "Total realised profit", "ETF exposure", "Future exposure",
                                   "Our bid", "Our ask", "Orderbook average bid", "Orderbook average ask",
                                   "Orderbook leading bid", "Orderbook leading ask"]
        log_file = open(self.__log_file_name, "w")
        for column_name in self.__log_file_columns:
            log_file.write(column_name + ",")
        log_file.write("\n")
        log_file.close()

    def log(self):
        log_file = open(self.__log_file_name, "a")

        # write data for each required column
        for column_name in self.__log_file_columns:
            write_text = ""
            if column_name == "Sequence number":
                write_text = SequenceNumber
            elif column_name == "ETF realised profit":
                write_text = ETFMarket.get_realised_profit()
            elif column_name == "Futures realised profit":
                write_text = FUTUREMarket.get_realised_profit()
            elif column_name == "Total realised profit":
                write_text = FUTUREMarket.get_realised_profit() + ETFMarket.get_realised_profit()
            elif column_name == "ETF exposure":
                write_text = ETFMarket.get_position()
            elif column_name == "Future exposure":
                write_text = FUTUREMarket.get_position()
            elif column_name == "Orderbook average bid":
                write_text = InternalOrderBook.get_avg_bid_ask()[0]
            elif column_name == "Orderbook average ask":
                write_text = InternalOrderBook.get_avg_bid_ask()[1]
            elif column_name == "Orderbook leading bid":
                write_text = InternalOrderBook.leading_bid_ask()[0]
            elif column_name == "Orderbook leading ask":
                write_text = InternalOrderBook.leading_bid_ask()[1]
            elif column_name == "Our bid":
                write_text = ETFMarket.best_price(Side.BID)
            elif column_name == "Our ask":
                write_text = ETFMarket.best_price(Side.ASK)

            log_file.write(str(write_text) + ",")

        log_file.write("\n")
        log_file.close()


class OrderBook:
    # keeps track of the orderbook internally
    def __init__(self):
        self.bid_prices = []
        self.bid_volumes = []
        self.ask_prices = []
        self.ask_volumes = []
        self.filled_order_sides = []  # keeps track of what order side was filled to detect prolonged market moves

    def book_update(self, bid_ps, bid_vs, ask_ps, ask_vs):
        self.bid_prices = bid_ps
        self.bid_volumes = bid_vs
        self.ask_prices = ask_ps
        self.ask_volumes = ask_vs

    def get_bid_ask(self):
        # calculate fair value, spread, and bid/ask prices
        averageBid, averageAsk = self.get_avg_bid_ask()

        fair_value = weighted_average([averageBid, averageAsk], [sum(self.ask_volumes), sum(self.bid_volumes)])

        # spread
        average_spread = (averageAsk - averageBid)  # avg spread implied by orderbook
        spread = average_spread * TraderParameters.SPREAD_FACTOR

        return [fair_value - spread / 2, fair_value + spread / 2]

    def get_avg_bid_ask(self):
        averageAsk = weighted_average(self.ask_prices, self.ask_volumes)
        averageBid = weighted_average(self.bid_prices, self.bid_volumes)
        return averageBid, averageAsk

    def leading_bid_ask(self):
        if not (len(self.bid_prices) and len(self.ask_prices)):
            return [0, 0]
        return self.bid_prices[0], self.ask_prices[0]


class Parameters:
    # Tune - able parameters
    def __init__(self):

        # order price setting
        self.SPREAD_FACTOR = 0.7

        # order size
        # we place orders of size at most LOT_SIZE orders on each side, with at most MAX_ORDERS
        self.LOT_SIZE = 15  # size of bid/ask positions
        self.MAX_ORDERS = 10 * self.LOT_SIZE  # max number of orders on either side
        self.SOFT_POS_LIMIT = 50  # used when improving order competitiveness

        # updating old orders
        # we don't like cancelling old orders, instead
        self.MAX_TICK_DEVIATION = 5  # deviation at which we cancel old orders

        # hedging parameters
        # vanilla hedging means we fully hedge every filled trade. non-vanilla means we do fancy hedging to reduce costs
        self.VANILLA_HEDGING = False
        self.MIN_HEDGE_COMPLETELY = 15  # max exposure at which we fully re-hedge
        self.MAX_TIME_UNHEDGED = 30  # max number of ticks when we're not hedged to zero, before we hedge back to zero

        # safety checks
        # max number of orders we can fill on one side consecutively before we stop posting new orders
        self.MAX_ORDERS_IN_ROW = int(1000 / self.LOT_SIZE)


OverallStats = Statistics()
TraderParameters = Parameters()
ETFMarket = Market("ETF")
FUTUREMarket = Market("Futures")
SequenceNumber = 0
InternalOrderBook = OrderBook()


def weighted_average(arr, weights):
    # returns the average of the arr, weighted by the weights.
    if sum(weights) == 0:
        return 0
    return sum([x * w for x, w in zip(arr, weights)]) / sum(weights)


def round_to_tick_size(x):
    return round(x // TICK_SIZE_IN_CENTS) * TICK_SIZE_IN_CENTS


def remove_zeroes(arr):
    return [x for x in arr if x != 0]


def get_priority_price(prices, volumes, priority):
    # returns the price needed to have our orders be at least top 'priority' in the book

    total_volume = 0
    prev_price = prices[0]
    for price, volume in zip(prices, volumes):
        total_volume += volume
        if total_volume >= priority:
            return prev_price

        prev_price = price


def avg(arr):
    if len(arr) == 0:
        return
    return sum(arr) / len(arr)


class AutoTrader(BaseAutoTrader):
    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)

        # limit frequency of orders
        self.frequency_limiter = FrequencyLimiter(1.0, 40)

        # keep track of how long we've been unhedged
        self.first_unhedged = 0

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())

        if ETFMarket.contains_id(client_order_id):
            self.on_order_status_message(client_order_id, 0, 0, 0)
        if FUTUREMarket.contains_id(client_order_id):
            self.on_hedge_filled_message(client_order_id, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        FUTUREMarket.order_filled(client_order_id, volume_filled=volume, price=price)

    def within_message_limit(self):
        # checks we aren't sending too frequent messages to the exchange
        OverallStats.total_limiter_calls += 1

        if self.frequency_limiter.check_event(time.time()):
            OverallStats.total_limiter_cutoffs += 1
            return False
        return True

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        self.logger.info("      ORDER STATUS")

        # It could be the case that an order is cancelled, but filled before that cancel goes through.
        # So we cancel orders in the on_order_filled function, and also here.
        if remaining_volume == 0:
            ETFMarket.cancel_order(client_order_id)

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info("      TRADE TICKS")

        pass

    """Trading logic"""

    def do_hedge_order(self, volume):
        # check position limits
        if not max(abs(FUTUREMarket.potential_positive_exposure() + volume),
                   abs(FUTUREMarket.potential_negative_exposure() + volume)) < POSITION_LIMIT:
            return

        if volume > 0:
            # if volume > 0, we are buying futures

            # send the hedge order internally and externally
            order_id = next(self.order_ids)
            self.send_hedge_order(order_id, Side.BID, MAX_ASK_NEAREST_TICK, volume)
            FUTUREMarket.create_order(order_id, Side.BID, volume, 0)

        elif volume < 0:
            # if volume < 0, we are selling futures
            futures_volume = -volume

            # send the hedge order internally and externally
            order_id = next(self.order_ids)
            self.send_hedge_order(order_id, Side.ASK, MIN_BID_NEAREST_TICK, futures_volume)
            FUTUREMarket.create_order(order_id, Side.SELL, futures_volume, 0)

    def hedge_completely(self):
        # check message limits
        if not self.within_message_limit():
            return

        # Hedge our position.
        # When we are hedging, we will never be able to hedge perfectly, so we have a margin for error
        total_netted_position = ETFMarket.get_position() + (
                FUTUREMarket.get_position() + FUTUREMarket.num_order(Side.BID) - FUTUREMarket.num_order(Side.ASK))

        self.do_hedge_order(-total_netted_position)

    def send_etf_order(self, side, volume, price):
        # check message limits
        if not self.within_message_limit():
            return

        # check price limit
        if (price <= MINIMUM_BID) or (price >= MAXIMUM_ASK):
            return

        order_id = next(self.order_ids)

        self.send_insert_order(order_id, side, price, volume, Lifespan.GOOD_FOR_DAY)  # send the order to the exchange
        ETFMarket.create_order(order_id, side, volume, price)  # update our internal order book

    def cancel_etf_order(self, order_id):
        # check message limits
        if not self.within_message_limit():
            return

        self.send_cancel_order(order_id)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        self.logger.info("[ORDER BOOK]")

        # remove the zeroes
        ask_prices = remove_zeroes(ask_prices)
        bid_prices = remove_zeroes(bid_prices)

        # update sequence number (i.e. what time is it)
        global SequenceNumber
        SequenceNumber = sequence_number

        # update our log file
        OverallStats.log()

        # are we using fancy hedging?
        if not TraderParameters.VANILLA_HEDGING:
            total_netted_position = ETFMarket.get_position() + FUTUREMarket.get_position() + \
                                    FUTUREMarket.num_order(Side.BID) - FUTUREMarket.num_order(Side.ASK)
            if total_netted_position == 0:
                self.first_unhedged = SequenceNumber
            elif abs(total_netted_position) > 0:
                if SequenceNumber - self.first_unhedged > TraderParameters.MAX_TIME_UNHEDGED:
                    self.hedge_completely()
                    self.first_unhedged = SequenceNumber

        # check if any of the arrays are empty, in which case we can't execute our strategy
        if (not len(ask_prices)) or (not len(bid_prices)):
            return

        # if we have received price data for the ETF
        if instrument == Instrument.ETF:
            """
            Calculate Bid and ask spreads
            """
            # save the orderbook internally
            InternalOrderBook.book_update(bid_prices, bid_volumes, ask_prices, ask_volumes)
            bid, ask = InternalOrderBook.get_bid_ask()

            """
            Adjust prices to be more competitive to limit exposure
            """
            spread = ask - bid

            ask -= min((max(ETFMarket.get_position(), 0) / TraderParameters.SOFT_POS_LIMIT) ** 2, 1) * spread / 2
            bid += min(((max(-ETFMarket.get_position(), 0) / TraderParameters.SOFT_POS_LIMIT) ** 2), 1) * spread / 2

            """
            Deal with any existing orders
            """
            # see if there are any stale orders that we need to cancel, or make more competitive
            for order_id in ETFMarket.get_id_list(Side.BID):
                order = ETFMarket.get_order(order_id)

                # cancel if deviates too far from ideal postings
                if abs(order.price - bid) > TraderParameters.MAX_TICK_DEVIATION * TICK_SIZE_IN_CENTS:
                    self.cancel_etf_order(order_id)

            for order_id in ETFMarket.get_id_list(Side.ASK):
                order = ETFMarket.get_order(order_id)

                # cancel if deviates too far from ideal postings
                if abs(order.price - ask) > TraderParameters.MAX_TICK_DEVIATION * TICK_SIZE_IN_CENTS:
                    self.cancel_etf_order(order_id)

            """
            Calculate volumes
            """
            bid, ask = round_to_tick_size(bid), round_to_tick_size(ask)
            # check that we are within position limits, and send orders
            bid_volume = min(TraderParameters.LOT_SIZE,
                             TraderParameters.MAX_ORDERS - ETFMarket.num_order(Side.BID),
                             POSITION_LIMIT - 1 - ETFMarket.potential_positive_exposure())
            ask_volume = min(TraderParameters.LOT_SIZE,
                             TraderParameters.MAX_ORDERS - ETFMarket.num_order(Side.ASK),
                             POSITION_LIMIT - 1 + ETFMarket.potential_negative_exposure())

            """
            Check that we haven't filled too many orders on one side. this could be a sign of a prolonged market move
            """
            prolonged_market_move = ETFMarket.detect_market_move()
            if prolonged_market_move != -5:
                self.hedge_completely()
                side_string = "Bid" if prolonged_market_move == Side.BID else "Ask"
                print(f"Prolonged move detected {SequenceNumber} in direction {side_string}")
                if prolonged_market_move == Side.BID:
                    bid_volume = 0
                elif prolonged_market_move == Side.ASK:
                    ask_volume = 0

            """
            Finally send of the orders!
            """
            if bid_volume > 0:
                self.send_etf_order(Side.BID, bid_volume, bid)
            if ask_volume > 0:
                self.send_etf_order(Side.ASK, ask_volume, ask)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        # update our internal order book
        ETFMarket.order_filled(order_id=client_order_id, volume_filled=volume, price=price)

        total_netted_position = ETFMarket.get_position() + FUTUREMarket.get_position() + \
                                FUTUREMarket.num_order(Side.BID) - FUTUREMarket.num_order(Side.ASK)

        # do some hedging
        if TraderParameters.VANILLA_HEDGING:
            self.do_hedge_order(-total_netted_position)
        else:
            if abs(total_netted_position) > TraderParameters.MIN_HEDGE_COMPLETELY:
                self.do_hedge_order(-total_netted_position)
