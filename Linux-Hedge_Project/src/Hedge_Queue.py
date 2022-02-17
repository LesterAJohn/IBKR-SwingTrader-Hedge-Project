from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
from ibapi.order import Order
from ibapi.common import SetOfString
from ibapi.common import SetOfFloat
from ibapi.common import BarData
from threading import Timer, Event, activeCount
from numpy import double, str0
from datetime import date, datetime, timedelta
from random import randrange
from logging.handlers import RotatingFileHandler
from ib_insync.order import Trade, OrderStatus
from ib_insync.objects import Position

import queue
import math
import string
import time
import datetime
import random
import threading
import numpy as np
import logging
import sys
import getopt
import configparser
import pymongo

random.seed(a=None, version=2)
event = threading.Event()

configParser = configparser.RawConfigParser()
configFilePath = '../../env/Env.conf'
configParser.read(configFilePath)

localHostAccount = configParser.get('ACCOUNT', 'IBKR_ACCT')
twsHost = configParser.get('ACCOUNT', 'TWSHOST')
mdLocalHostAccount = configParser.get('ACCOUNT', 'MARKETDATA_IBKR_ACCT')
mdTwsHost = configParser.get('ACCOUNT', 'MARKETDATA_TWSHOST')
twsPort = int(configParser.get('ACCOUNT', 'TWSPORT'))
connectId = int(configParser.get('ACCOUNT', 'CONNECTID'))
cycleTime = int(configParser.get('ACCOUNT', 'CYCLETIME'))
cycleDisTime = int(configParser.get('ACCOUNT', 'CYCLEDISTIME'))

targetPnLTrigger = float(configParser.get('OPTION', 'PNLTRIGGER'))
targetPnLTriggerPer = float(configParser.get('OPTION', 'PNLTRIGGERPER'))
hedgePercentage = configParser.get('OPTION', 'HEDGEPERCENTAGE')
pt = configParser.get('OPTION', 'PROFITTARGET')
minExp = float(configParser.get('OPTION', 'MINEXPTIME'))
lookAheadDays = float(configParser.get('OPTION', 'LOOKAHEADDAYS'))
historyLoop = float(configParser.get('OPTION', 'HISTORYLOOP'))
historyReq = float(configParser.get('OPTION', 'HISTORYREQ'))
historyDelay = float(configParser.get('OPTION', 'HISTORYDELAY'))

dbPath = configParser.get('DIRECTORY', 'DB')
logsPath = configParser.get('DIRECTORY', 'LOGS')
mongodbConn = configParser.get('DIRECTORY', 'MONGODBCONN')
mongoDB = configParser.get('DIRECTORY', 'MONGODB')

hedgePercentage = (float(hedgePercentage)/100)
profitTarget = ((100 - int(pt))/100) 
OID = 0
seqCount = 0
netLiq = 0
buyPower = 0
runActive = True
activeHisCount = 0
processQ = []
QCount = ""
QFunction = ""


class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        Mongodb.__init__(self)

    def error(self, reqId, errorCode, errorString):
        if reqId > -1:
            print("Error: ", reqId, " ", errorCode, " ", errorString)
            if(errorCode == 162):
                global activeHisCount
                activeHisCount = activeHisCount - 1
                if(reqId <= 50000):
                    DBApp.resAskBidAcctRecord(self, reqId, -1.00, -1.00)
                    DBAppOption.resAskBidOptionRecord(self, reqId, -1.00, -1.00)
                if(reqId > 50000):
                    pass
                processQueue.reqHistoricalDataRemoveQueue(self, reqId, None)

    def nextValidId(self, orderId):
        if (orderId == 0):
            self.stop()
        else:
            log.info("nextValidId: " + str(orderId))
            global OID
            OID = orderId
            log.info("Transfer nextValidId transfered to OID: " + str(OID))
            self.start()

# IBAPI Response One-time
    def securityDefinitionOptionParameter(self, reqId:int, exchange:str,
        underlyingConId:int, tradingClass:str, multiplier:str,
        expirations:SetOfString, strikes:SetOfFloat):
        print("SecurityDefinitionOptionParameter.", "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier, "Expirations:", expirations, "Strikes:", str(strikes),"\n")

    def securityDefinitionOptionParameterEnd(self, reqId:int):
        print("SecurityDefinitionOptionParameterEnd. ReqId:", reqId)
        
    def position(self, account, contract, position, avgCost):
        if (abs(position) >= 100) or (contract.secType == 'OPT') or (contract.secType == 'CRYPTO') or (position == 0):
            log.info("Position." + "Account: " + account + " Symbol: " + contract.symbol + " ConId " + str(contract.conId) + " SecType: " + contract.secType +  " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(position) + " Avg cost: " + str(avgCost) + " Right: " + contract.right + " Strike: " + str(contract.strike))
            DBApp.resAddAcctRecord(self, account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, position, avgCost, contract.right, contract.strike, contract.primaryExchange)

    def positionEnd(self):
        log.info("Position Download End")

    def contractDetails(self, reqId, contractDetails):
        #log.info("contractDetails: ", reqId, " ", contractDetails, "\n")
        log.info("contractDetails: " + str(reqId) + " " + contractDetails.contract.symbol + " " + str(contractDetails.contract.conId) + " " + contractDetails.contract.secType + " " +  contractDetails.contract.right + " " + str(contractDetails.contract.strike))
        try:
            loop_contractDetailsStage = threading.Thread(target=self.contractDetailsStage, args=(reqId, contractDetails))
            loop_contractDetailsStage.start()
        except Exception as e:
            log.info("contractDetails ERROR Captured " + str(e))

    def contractDetailsStage(self, reqId, contractDetails):
        #log.info("contractDetails: ", reqId, " ", contractDetails, "\n")
        if(reqId <= 50000):
            DBAppOption.resOptionsInfo(self, reqId, contractDetails.contract.symbol, contractDetails.contract.conId, contractDetails.contract.secType, contractDetails.contract.lastTradeDateOrContractMonth, contractDetails.contract.right, contractDetails.contract.strike)
        if (reqId > 50000):
            processQueue.reqHistoricalDataQueue(self, reqId, contractDetails.contract.symbol, contractDetails.contract.conId, contractDetails.contract.secType, 'Account')

    def contractDetailsEnd(self, reqId):
        log.info("contractDetails Download End")
        try:
            loop_contractDetailsEndStage = threading.Thread(target=self.contractDetailsEndStage(reqId))
            loop_contractDetailsEndStage.start()
        except Exception as e:
            log.info("contractDetailsEnd ERROR Captured " + str(e))

    def contractDetailsEndStage(self, reqId):
        log.info("contractDetails Download End " + str(reqId))
        DBAppOption.resOptionsInfoComplete(self, reqId)

    def historicalData(self, reqId:int, bar:BarData):
        log.info("HistoricalData. ReqId :" + str(reqId) + " BarData. " + str(bar))
        try:
            loop_historicalDataStage = threading.Thread(target=self.historicalDataStage, args=(reqId, bar))
            loop_historicalDataStage.daemon = True
            loop_historicalDataStage.start()
        except Exception as e:
            log.info("historicalData ERROR Captured " + str(e))
                    
    def historicalDataStage(self, reqId:int, bar:BarData):
        if(reqId <= 50000):
            DBApp.resAskBidAcctRecord(self, reqId, bar.high, bar.low)
            DBAppOption.resAskBidOptionRecord(self, reqId, bar.high, bar.low)
        if (reqId > 50000):
            DBApp.resAskBidAcctRecord(self, reqId, bar.high, bar.low)
    
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        global activeHisCount
        activeHisCount = activeHisCount - 1
        super().historicalDataEnd(reqId, start, end)
        log.info("HistoricalDataEnd. ReqId: " + str(reqId) + " from " + str(start) + " to " + str(end) + " Queue Length " + str(activeHisCount))
        try:
            loop_historicalDataEndStage = threading.Thread(target=self.historicalDataEndStage, args=(reqId, start, end))
            loop_historicalDataEndStage.daemon = True
            loop_historicalDataEndStage.start()
        except Exception as e:
            log.info("historicalDataEnd ERROR Captured " + str(e))
        
    def historicalDataEndStage(self, reqId: int, start: str, end: str):
        if(reqId <= 50000):
            pass
        if(reqId > 50000):
            pass
        processQueue.reqHistoricalDataRemoveQueue(self, reqId, None)
        if (activeHisCount == 0):
            processQueue.reqHistoricalDataResetQueue(self)
        
# IBAPI Response Subscription
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        log.info("HistoricalDataUpdate. ReqId: " + str(reqId) + " BarData. " + str(bar.high) + str(bar.low))
        
    def accountSummary(self, reqId:int, account:str, tags:str, value:str, currency:str):
        global netLiq
        global buyPower
        log.info("Account Info: " +  account + " : " + tags + " : " + value)
        if(tags == "BuyingPower"):
            log.info("Updated BuyPower Value: " +  account + " : " + tags + " : " + value)
            buyPower = float(value)
            if (buyPower > 1000):
                self.reqOpenOrders()
                loop(5)
                DBOrder.reqOptionOrderEval(self)
                loop(5)
                DBOrder.reqOptionOrderCreate(self)
            
    def accountSummaryEnd(self, reqId:int):
        log.info("Account Summary End")
        
    def updateAccountValue(self, key, value, currency, accountName):
        print (key, value)
    
    def positionMulti(self, reqId: int, account: str, modelCode: str, contract: Contract, pos: int, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
        if (abs(pos) >= 100) or (contract.secType == 'OPT') or (pos == 0):
            log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
            try:
                loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                loop_positionMultiStage.daemon = True
                loop_positionMultiStage.start()
            except Exception as e:
                log.info("Position Thread ERROR Captured " + str(e))
            
    def positionMultiStage(self, account, symbol, conId, secType, currency, lastTradeDateOrContractMonth, pos, avgCost, right, strike, exchange):
        DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, lastTradeDateOrContractMonth, pos, avgCost, right, strike, exchange)
        #if(pos == 0):
        #    self.reqAcctPnLdisable(conId)
        #else:
        #    self.reqAcctPnL(conId)

    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        log.info("PositionMultiEnd. RequestId: " + str(reqId))

    def pnlSingle(self, reqId:int, pos:int, dailyPnL:float, UnrealizedPnL:float, realizedPnL:float, value:float):
        log.info("Daily PnL Single Subscription. ReqId: " + str(reqId) + " Position: " + str(pos) + " DailyPnL: " + str(dailyPnL) + " UnrealizedPnL: " + str(UnrealizedPnL) +  " RealizedPnL: " + str(realizedPnL) + " Value: " + str(value))
        try:
            loop_reqPnLStage = threading.Thread(target=self.reqPnLStage, args=(reqId, UnrealizedPnL, pos))
            loop_reqPnLStage.daemon = True
            loop_reqPnLStage.start()
        except Exception as e:
            log.info("PnLStage Thread ERROR Captured " + str(e))
        
    def reqPnLStage(self, reqId, UnrealizedPnL, position):
        DBApp.resPnLUpdateAcctRecord(self, reqId, UnrealizedPnL, position)
        Mongodb.reqAskBidAcctRecord(self, reqId)
        Mongodb.reqStockPriceAcctRecord(self, reqId)
        Mongodb.reqOptionCloseAcctRecord(self, reqId)
        DBLogic.logic_evaluateOption_positionSize(self, reqId)
        Mongodb.reqHedgeStatusAcctRecord(self, reqId)
        DBLogic.logicSelectOptionTargets(self, reqId)
        Mongodb.reqContractDownloadAcctRecord(self, reqId)
        Mongodb.reqAskBidOptions(self, reqId)
        
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        log.info("HistoricalDataUpdate. ReqId: " + str(reqId) + " BarData. " + str(bar.high) + str(bar.low))
        
# IBAPI Order Response
    def openOrder(self, orderId, contract, order, orderState):
        log.info("openOrder id: " + str(orderId) + " " + contract.symbol + " " + contract.secType + " @ " + contract.exchange + " : " + order.action + " " + order.orderType + " " + str(order.totalQuantity) + " " + orderState.status)
        DBOrder.resOptionOrderStatus(self, contract.conId, order.orderId, orderState.status)
        
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)       
        #print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled, "Remaining:", remaining, "AvgFillPrice:", avgFillPrice, "PermId:", permId, "ParentId:", parentId, "LastFillPrice:", lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld, "MktCapPrice:", mktCapPrice)
        DBOrder.resOptionOrderStatus(self, 'None', orderId, status)

    def execDetails(self, reqId, contract, execution):
        log.info("Order Executed: " + str(reqId) + " " + contract.symbol + " " + contract.secType + " " + contract.currency  + " " + str(execution.execId) + " " + str(execution.orderId) + " " + str(execution.shares) + " " + str(execution.lastLiquidity))
        if(execution.shares > 0):
            DBOrder.resOptionOrderStatus(self, contract.conId, execution.orderId, 'Filled')
        
    def completedOrder(self, contract, order, orderState):
        log.info ("completedOrder Symbol: " + contract.symbol + " conId: " + str(contract.conId) + " OrderId: " + str(order.orderId) + " OrderState: " + orderState.status)
        DBOrder.resOptionOrderStatus(self, contract.conId, order.orderId, orderState.status)

    def openOrderEnd(self):
        log.info("openOrderEnd Update")

    def completedOrdersEnd(self):
        log.info("Completed Orders")
    
# IBAPI Request
    def getAcctPnL(self, reqId, conId):
        log.info("getAcctPnL- " + "ReqId: " + str(reqId) + " conId: " + str(conId))
        self.reqPnLSingle(reqId, localHostAccount, "", conId)
            
    def acct_Position(self):
        log.info("Get Account Positions")
        self.reqPositions()
        
    def nextOrderId(self):
        global OID
        self.reqIds(0)
        
    def getContractDetails_optionPrice(self, symbol, secType, monthExp, realTimeNum):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = "SMART"
        contract.currency = "USD"
        #contract.conId = "317033694"
        contract.lastTradeDateOrContractMonth = monthExp  # October 2020
        log.info("getContractDetails_optionPrice: " + "ReqId: " + str(realTimeNum) + " Contract: " + str(contract))
        self.reqContractDetails(realTimeNum, contract)
        
    def getContractDetails_stockPrice(self, symbol, realTimeNum):
        realTimeNum = realTimeNum
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = "USD"
        #contract.conId = "317033694"
        #contract.lastTradeDateOrContractMonth = monthExp  # October 2020
        log.info("getContractDetails_stockPrice: " + "ReqId: " + str(realTimeNum) + " Contract: " + str(contract))
        self.reqContractDetails(realTimeNum, contract)
        
    def getAskBid(self, reqId, symbol, conId, secType):
        queryTime = datetime.datetime.today().strftime("%Y%m%d %H:%M:%S")
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = "SMART"
        #contract.currency = "USD"
        contract.conId = conId
        #contract.lastTradeDateOrContractMonth = monthExp  # October 2020
        global activeHisCount
        activeHisCount = activeHisCount + 1
        log.info("getAskBid " + " Queue Size: " + str(activeHisCount) + " ReqId: " + str(reqId) + "  " +  str(contract))
        self.reqHistoricalData(reqId, contract, '', "60 S", "1 MIN", "MIDPOINT", 0, 1, False, [])

    def orderEntry(self, orderId, contract, order):
        self.placeOrder(orderId, contract, order)
        #self.reqAllOpenOrders()
        
    def orderCancel(self, orderId):
        self.cancelOrder(orderId)

    def start(self):
        global seqCount
        seqCount = seqCount + 1
        log.info ("Global Sequence Count: " + str(seqCount) + " Queue Process Number: " + str(QCount) + " Queue Function: " + QFunction)

        try:
            if (QFunction == "order"):
                self.reqAccountSummary(100001, "All", "BuyingPower")
                self.reqPositionsMulti(100002, localHostAccount, "")
        except Exception as e:
            log.info("Account Subscription " + str(e))

        try:
            if (QFunction == "order"):
                loop_orderStatusLoad = threading.Thread(target=self.orderStatusLoad_Loop)
                loop_orderStatusLoad.start()
        except Exception as e:
            log.info("Account Order Status ERROR Captured " + str(e))
            
        try:
            if (QFunction == "order"):
                loop_optionCancel = threading.Thread(target=self.optionCancel_Loop)
                loop_optionCancel.start()
        except Exception as e:
            log.info("option cancel ERROR Capture " + str(e))
        
        try:
            loop_checkConnection = threading.Thread(target=self.connectStatus_Loop)
            loop_checkConnection.start()
        except Exception as e:
            log.info("connection check ERROR Capture " + str(e))
            
        try:
            if (QFunction == "account" or QFunction == "option"):
                loop_reqHistorical = threading.Thread(target=self.historical_Loop)
                loop_reqHistorical.start()
        except Exception as e:
            log.info("Historical Request ERROR Capture " + str(e))
    
    def orderStatusLoad_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=180)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Order Management Loop")
                self.reqOpenOrders()
                loop(5)
                DBOrder.reqOptionOrderEval(self)
                loop(5)
                DBOrder.reqOptionOrderCreate(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=180)
            loop(1)
            
    def optionCancel_Loop(self):
        nextX = datetime.datetime.now()
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Option Order Cancel Loop")
                DBOrder.req_optionOrder_activeCancel(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=300)
            loop(1)
                   
    def connectStatus_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=180)
        global runActive
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                if (self.isConnected() == False):
                    runActive = False
                log.info("Checking the status of the API Connection: " + str(runActive))
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=180)
            loop(1)
            
    def historical_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=historyLoop)
        global runActive
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Historical Data Request Loop")
                processQueue.reqHistoricalDataFromQueue(self, historyDelay)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=historyLoop + randrange(60))
            loop(1)
    
    def reqAcctPnL(self, conId):
        log.info("Request Positions PnL")
        Mongodb.reqPnLAcctRecord(self, conId)
        
    def reqAcctPnLdisable(self, conId):
        log.info("Disabled Position PnL")
        Mongodb.reqPnlsubDisableAcctRecord(self, conId)
        processQueue.reqHistoricalDataRemoveQueue(self, None, conId)

    def sub_stop(self, reqId):
        log.info("Disconnect " + str(reqId))
        self.cancelPnLSingle(reqId)
        
    def stop(self):
        self.disconnect()


class Mongodb(IBApp):
    def __init__(self):
        self.client = pymongo.MongoClient(mongodbConn, maxPoolSize=900)
        self.db = self.client[mongoDB]

    # General Functions        
    def showCollections(self):
        print(self.db.list_collection_names())
        
    def insertDocument(self, collection, data):
        activeCol = self.db[collection]     
        activeCol.insert_one(data)
               
    def recordQuery(self, collection, query):
        record = {}
        activeCol = self.db[collection]
        record = activeCol.find_one(query)
        return (record)
    
    def recordQueries(self, collection, query):
        record = {}
        activeCol = self.db[collection]
        record = activeCol.find_all(query)
        return (record)
    
    def recordUpdate(self, collection, query, data):
        record = {}
        activeCol = self.db[collection]
        activeCol.update_one(query, { "$set" : data })

    def recordUpdates(self, collection, query, data):
        activeCol = self.db[collection]
        activeCol.update_many( query, { "$set" : data })
        
    def updateAcctRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_one(query, update_data)

    #Hedge Project Specific
    def addUniqueRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        if(self.recordQuery(collection, query) == None):
            activeCol.insert_one(data)
        else:
            update_data = {"$set": update_data}
            activeCol.update_one(query, update_data)
            
    def reqPnLAcctRecord(self, conId):
        activeCol = self.db['Account']
        query = {"subPnL" : False, "subPnLRequest" : False, "status" : True,  "conId" : conId}
        for r in activeCol.find(query):
            log.info("Subscribe to PnL for position: " + r.get('symbol'))
            reqId = r.get('realTimeNum')
            activeCol.update_one({'conId' : conId}, {"$set":{'subPnLRequest' : True}})
            IBApp.getAcctPnL(self, reqId, conId)
            
    def reqPnlsubDisableAcctRecord(self, conId):
        activeCol = self.db['Account']
        query = {"subPnL" : True, "subPnLRequest" : True, "status" : False, "conId" : conId}
        for r in activeCol.find(query):
            log.info("unSubscribe PnL for Position: " + r.get('symbol'))
            reqId = r.get('realTimeNum')
            activeCol.update_one({'conId' : conId}, {"$set":{'subPnLRequest' : False}})
            IBApp.sub_stop(self, reqId)  
            
    def reqStockPriceAcctRecord(self, reqId):
        activeCol = self.db['Account']
        query2 = {"status" : True, "secType" : "OPT", "ask": {"$ne" : 0}, "bid" : {"$ne" : 0}, "realTimeNum" : reqId}
        for rr in activeCol.find(query2):
            realTimeNum = reqId + 50000
            symbol = rr.get('symbol')
            IBApp.getContractDetails_stockPrice(self, symbol, realTimeNum)
        

class DBApp(IBApp):
    def resAddAcctRecord(self, account, symbol, conId, secType, currency, expDate, position, avgCost, right, strike, exchange):
        db = Mongodb()
        TDate_obj = date.today()
        TDate_str = datetime.datetime.strftime(TDate_obj, '%Y%m%d')
        RTNum = DBLogic.random_RTP(self)
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        
        if (len(expDate) > 0):
            expDate_obj = datetime.datetime.strptime(expDate, '%Y%m%d')
        else:
            expDate_obj = '00000000'
            
        if (exchange == "" and secType == "STK"):
            exchange = "SMART"
            
        if (exchange == "" and secType == "OPT"):
            exchange = "SMART"
            
        if (exchange == "" and secType == "CRYPTO"):
            exchange = "PAXOS"
        
        query = { 'conId' : conId }
        data = { 'account' : account, 'symbol' : symbol, 'conId' : conId, 'secType' : secType, 'currency' : currency, 'exchange' : exchange,
                'position' : position, 'avgCost' : avgCost, 'expDate' : expDate_obj, 'right' : right, 'strike' : strike, 
                'ask' : 0.00, 'bid' : 0.00, 'positionPrice' : 0.00, 'stockPrice' : 0.00, 'priceDate' : priceDate_str, 'subPnL' : False, 'subPnLRequest' : False,
                'hedge' : False, 'optionDownload' : False, 'optionDownloadActive' : False, 'AskBidActive' : False,
                'recDate' : TDate_str, 'realTimeNum' : RTNum, 'status' : True }
        if(position != 0):
            data['status'] = True
            update_data = {"position" : data['position'], "avgCost" : data['avgCost'], "status" : data['status'], "priceDate" : data['priceDate']}
            db.addUniqueRecord('Account', query, data, update_data)
        else:
            data['status'] = False
            update_data = {"position" : data['position'], "avgCost" : data['avgCost'], "status" : data['status'], "priceDate" : data['priceDate']}
            db.addUniqueRecord('Account', query, data, update_data)
        
    def resPnLUpdateAcctRecord(self, reqId, unRealizedPnL, position):
        db = Mongodb()
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        query = {'realTimeNum' : reqId}
        data = {'unRealizedPnL' : unRealizedPnL, "position" : position, "priceDate" : priceDate_str}
        update_data = {"unRealizedPnL" : data['unRealizedPnL'], "subPnL" : True, "position" : data['position'], "priceDate" : data['priceDate']}
        db.updateAcctRecord('Account', query, data, update_data)

    def resAskBidAcctRecord(self, reqId, ask, bid):
        db = Mongodb()
        positionPrice = DBLogic.logic_midPrice_Calculation(ask, bid)
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        if(reqId <= 50000):
            query = { "realTimeNum" : reqId }
            data = { "ask" :  ask, "bid" : bid, "priceDate" : priceDate_str, "positionPrice" : positionPrice }
            update_data = { "ask" :  data['ask'], "bid" : data['bid'], "priceDate" : data['priceDate'], "positionPrice" : data['positionPrice'] }
            db.updateAcctRecord('Account', query, data, update_data)
        if(reqId > 50000):
            reqId = reqId - 50000
            stockPrice = DBLogic.logic_midPrice_Calculation(ask, bid)
            query = {"realTimeNum" : reqId}
            data = {"stockPrice" :  stockPrice}
            update_data = {"stockPrice" : data['stockPrice']}
            db.updateAcctRecord('Account', query, data, update_data)


class DBAppOption(IBApp):
    def reqOptionsInfo(self, symbol, realTimeNum):        
        monthExp = (datetime.datetime.today() + datetime.timedelta(days=lookAheadDays)).strftime("%Y%m")
        secType = 'OPT'
        log.info("Request Option Chain " + symbol)
        IBApp.getContractDetails_optionPrice(self, symbol, secType, monthExp, realTimeNum)
            
    def resOptionsInfo(self, reqId, symbol, conId, secType, expDate_obj, right, strike):
        db = Mongodb()
        RTNum = DBLogic.random_RTP(self)
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        query = {'conId' : conId}
        data = {'symbol' : symbol, 'conId' : conId, 'secType' : secType, 'expDate' : expDate_obj, 'right' : right, 'strike' : strike, 
                'ask' : 0.00, 'bid' : 0.00, 'positionPrice' : 0.00, 'priceDate' : priceDate_int, 'aRealTimeNum' :  reqId, 'realTimeNum' : RTNum, 'request' : False}
        update_data = {"request" : data['request']}
        db.addUniqueRecord('Option', query, data, update_data)
        
    def resOptionsInfoComplete(self, reqId):
        db = Mongodb()
        query = { "realTimeNum" : reqId, "secType" : "STK"}
        data =  { "optionDownload" : True }
        update_data = {"optionDownload" : data['optionDownload']}
        log.info("Contract Download Ended " + str(reqId))
        db.updateAcctRecord('Account', query, data, update_data)

    def resAskBidOptionRecord(self, reqId, ask, bid):
        db = Mongodb()
        tDelta = DBAppOption.reqOptionsCount(self)
        priceDate_obj = datetime.datetime.now() + datetime.timedelta(seconds=tDelta)
        priceDate_str = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        if(reqId <= 50000):
            query = { "realTimeNum" : reqId }
            data = { "ask" :  ask, "bid" : bid, "positionPrice" : DBLogic.logic_midPrice_Calculation(ask, bid), "priceDate" : priceDate_int, "request" : False }
            update_data = { "ask" :  data['ask'], "bid" : data['bid'], "positionPrice" : data['positionPrice'], "priceDate" : data['priceDate'], "request" : data['request'] }
            db.updateAcctRecord('Option', query, data, update_data)

    def reqOptionsCount(self):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        priceDate_obj = datetime.datetime.now()
        priceDate_str = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        i = 0
        query = {"status" : True, "secType" : "STK", "hedge" : True, "optionDownload" : True}
        for r in activeCol.find(query):
            position = r.get('position')
            positionPrice = r.get('positionPrice')
            positionPrice_High = positionPrice + (positionPrice * hedgePercentage)
            positionPrice_Low = positionPrice - (positionPrice * hedgePercentage)
            if (position <= -100):
                right = "P"
            if (position >= 100):
                right = "C"
            query1 = {"symbol" : r.get('symbol'), "right" : right, "priceDate" : {"$lt" : priceDate_int}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}       
            i = i + activeCol1.count_documents(query1)
        
        i = ((i * (historyDelay * 1.2)) + minExp)
        if (i <= 0):
            i = minExp
        log.info("tDelta Compute time = " + str(round(abs(i),0)))
        return round(abs(i),0)


class DBLogic(IBApp):
    def random_RTP(self):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        activeCol2 = self.db['ProcessQueue']     
        
        RTNum = randrange(20000)
        
        query = { "realTimeNum" : RTNum }
        query1 = { "realTimeNum" : RTNum }
        query2 = { 'eventType':'Option Order', 'OrderNumId' : RTNum }
        
        while (activeCol.count_documents(query) > 0) or (activeCol1.count_documents(query1) > 0) or (activeCol2.count_documents(query2) > 0):
            RTNum = RTNum + 1
        return RTNum
            
    def logic_midPrice_Calculation(ask, bid):
        return ((ask+bid)/2)


class DBOrder(IBApp):
    def resOptionOrderStatus(self, conId, orderId, status):
        log.info("Phase 5 Order Update")
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        
        OrderTime_obj = datetime.datetime.now()
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        query = {'OrderNumId':orderId, 'OrderStatus':{"$ne":status}}
        data = {'OrderStatus':status, 'OrderTime':OrderTime_int}
        update_data = {'OrderStatus' :  data['OrderStatus'], 'OrderTime' : data['OrderTime']}       
        activeCol.update_one(query, {"$set": update_data})
        
    def reqOptionOrderEval(self):
        db = Mongodb()
        activeCol3 = self.db['ProcessQueue']
        
        log.info ("Order Record Cleanup")
        
        query = {'OrderStatus' : 'open'} 
        for r in activeCol3.find(query).sort('limitPrice', -1):
            conId = r.get('conId')
            symbol = r.get('symbol')
            
            query_count_conId = {'conId':conId, 'OrderStatus':"open"}
            if (activeCol3.count_documents(query_count_conId) != 1):
                activeCol3.delete_one(query_count_conId)
            
            query_count_symbol = {'symbol':symbol, 'OrderStatus':"open"}
            if (activeCol3.count_documents(query_count_symbol) != 1):
                activeCol3.delete_one(query_count_symbol)
        
    def reqOptionOrderCreate(self):
        db = Mongodb()
        activeCol3 = self.db['ProcessQueue']
        global OID
        OrderTime_obj = datetime.datetime.now()
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        
        log.info("Phase 4 Order DB Started")
        
        if(buyPower >= 1000.00):
            query = {'OrderStatus' : 'open', 'startTime':{"$lt": nowTime}, 'endTime':{"$gt": nowTime}}
            
            log.info ("Order records ready to be processed: " + str(activeCol3.count_documents(query)))
            
            for r in activeCol3.find(query):
                log.info("Phase 4 Order DB Submit: " + r.get('symbol') + " ContractId: " + str(r.get('conId')))
                
                contract = Contract()
                contract.symbol = r.get('symbol')
                contract.secType = r.get('secType')
                contract.exchange = r.get('exchange')
                contract.currency = r.get('currency')
                #contract.strike = r.get('strike')
                #contract.right = r.get('right')
                contract.conId = r.get('conId')
                
                order = Order()
                order.action = r.get('direction')
                order.orderType = r.get('orderType')
                if (r.get('orderType') == 'LMT'):
                    order.totalQuantity = r.get('limitPosition')
                    order.lmtPrice = round(r.get('limitPrice'),2)
                    order.tif = 'DAY'
                if (r.get('orderType') == 'MKT'):
                    order.cashQty = round(r.get('cashQty'),2)
                    order.tif = 'IOC'
                order.transmit = True
                order.outsideRth = False
                order.algoStrategy = r.get('algoStrategy')
                orderId = r.get('OrderNumId')
                
                log.info("OrderNumId Processing for: " + r.get('symbol'))
                
                if (orderId != OID):
                    activeCol3.update_one({'eventType': r.get('eventType'), 'conId':r.get('conId'), 'OrderStatus':'open'}, {"$set": {'OrderNumId':OID}})
                    orderId = OID
                    order.orderId = OID
                    OID = OID + 1
                    
                log.info("TRADE ORDER Entry: " + str(orderId) + " conId: " + str(r.get('conId')))
                activeCol3.update_one({'OrderNumId' : orderId, 'OrderStatus':'open', 'limitPrice':r.get('limitPrice')}, {"$set": {'OrderStatus' : 'Transmitted', 'OrderTime' : OrderTime_int}})
                loop(2)
                IBApp.orderEntry(self, orderId, contract, order)
                loop(2)
                break
            
    def req_optionOrder_activeCancel(self):
        db = Mongodb()
        activeCol2 = self.db['ProcessQueue']
        
        TDate_obj = date.today()
        TDate_str = datetime.datetime.strftime(TDate_obj, '%s')
        TDate_int = float(TDate_str)
        
        OrderTime_obj = datetime.datetime.now() - datetime.timedelta(seconds=300)
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        OrderTime1_obj = datetime.datetime.now() - datetime.timedelta(seconds=1200)
        OrderTime1_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime1_int = float(OrderTime_str)

        OrderTime2_obj = datetime.datetime.now() - datetime.timedelta(seconds=1500)
        OrderTime2_str = datetime.datetime.strftime(OrderTime1_obj, '%s')
        OrderTime2_int = float(OrderTime1_str)
        
        queryOrderRetry = {'OrderTime':{"$lt": OrderTime_int}, "OrderStatus" : {"$eq" : "Transmitted"}}
        query = {'OrderTime':{"$lt": OrderTime1_int}, "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}]}
        query1 = {'OrderTime':{"$lt": OrderTime2_int}, "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}]}      
        query2 = {"$or" : [{"recDate" : {"$lt" : TDate_int}}, {"OrderStatus" : {"$eq" : "Inactive"}}]}
        
        for rr in activeCol2.find(queryOrderRetry):
            log.info("Phase 6 Order")
            orderId = rr.get('OrderNumId')
            conId = rr.get('conId')
            IBApp.orderCancel(self, orderId)
            activeCol3.update_one({'OrderNumId' : orderId, 'conId': conId}, {"$set": {'OrderStatus' : 'open'}})
        
        for r in activeCol2.find(query):
            log.info("Phase 6 Order")
            orderId = r.get('OrderNumId')
            IBApp.orderCancel(self, orderId)
        loop(2)
        activeCol2.delete_many(query1)
        activeCol2.delete_many(query2)

class processQueue(IBApp):
    def clearProcessQueue(self):
        db = Mongodb
        activeCol = self.db['ProcessQueue']
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}],"$or" : [{'sent' : True}, {'sent' : False}]}
        activeCol.delete_many(query)
        log.info("ProcessQueue Collection Cleared")
    
    def reqHistoricalDataQueue(self, reqId, symbol, conId, secType, source):
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        dateTimeNow_obj = datetime.datetime.now() 
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        query = {'reqId' : reqId}
        if (source == 'Account'):        
            data = {'eventType':'Historical Account', 'reqId':reqId, 'symbol':symbol, 'conId':conId, 'secType':secType, 'recDate': dateTimeNow_int, 'lastDate': dateTimeNow_int, 'sent':False}
        if (source == 'Option'):
            data = {'eventType':'Historical Option', 'reqId':reqId, 'symbol':symbol, 'conId':conId, 'secType':secType, 'recDate': dateTimeNow_int, 'lastDate': dateTimeNow_int, 'sent':False}
        update_data = {'lastDate':data['lastDate']}
        db.addUniqueRecord('ProcessQueue', query, data, update_data)
        
    def reqHistoricalDataFromQueue(self, delay):
        db = Mongodb
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        if(QFunction  == "option"):
            query_core = {'status' : True, 'secType' : 'STK', 'hedge' :  True, 'optionDownload' : True, 'AskBidActive' : False}
            for sr in activeCol.find(query_core):           
                query_core1 = {'status' : True, 'secType' : 'STK', 'hedge' :  True, 'optionDownload' : True, 'realTimeNum' : sr.get('realTimeNum')}
                data_core1 = {'AskBidActive' : True}
                update_data_core1 = {"$set": {'AskBidActive': data_core1['AskBidActive']}}
                activeCol.update_one(query_core1, update_data_core1)
                
                query_opt = {'eventType': "Historical Option", 'symbol': sr.get('symbol'), 'sent': False}
                for r in activeCol1.find(query_opt).sort('recDate', 1):
                    IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'))
                    query_opt1 = {"reqId" : r.get('reqId')}
                    data_opt1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                    update_data_opt1 = {"$set":{'lastDate':data_opt1['lastDate'], 'sent':data_opt1['sent']}}
                    activeCol1.update_one(query_opt1, update_data_opt1)
                    loop(delay)
                    
                data_core1 = {'AskBidActive' : False}
                update_data_core1 = {"$set": {'AskBidActive': data_core1['AskBidActive']}}
                activeCol.update_one(query_core1, update_data_core1)

            # Account Queue Status Reset
            query_reset = {'status' : True, 'secType' : 'STK', 'hedge' :  True, 'optionDownload' : True, 'AskBidActive' : True}
            data_reset = {'AskBidActive' : False}
            update_reset = {"$set": {'AskBidActive':data_reset['AskBidActive']}}
            activeCol.update_many(query_reset, update_reset)
            
            # Option processQueue Reset
            query_reset1 = {'eventType':'Historical Option', 'sent' : True}
            data_reset1 = {'sent' : False}
            update_reset1 = {"$set": {'sent':data_reset1['sent']}}
            activeCol1.update_many(query_reset1, update_reset1)

        if(QFunction  == "account"):
            query_acct = {'eventType' : "Historical Account", 'sent' : False}
            for r in activeCol1.find(query_acct).sort('recDate', 1):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'))
                query_acct1 = {"reqId" : r.get('reqId')}
                data_acct1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_acct1 = {"$set":{'lastDate':data_acct1['lastDate'], 'sent':data_acct1['sent']}}
                activeCol1.update_one(query_acct1, update_data_acct1)
                loop(delay)
                
            # Option processQueue Reset
            query_reset1 = {'eventType':'Historical Account', 'sent' : True}
            data_reset1 = {'sent' : False}
            update_reset1 = {"$set": {'sent':data_reset1['sent']}}
            activeCol1.update_many(query_reset1, update_reset1)
            
    def reqHistoricalDataResetQueue(self):
        db = Mongodb
        activeCol = self.db['ProcessQueue']
        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'sent' : True}
        query1 = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'sent' : False}
        data = {'lastDate' :  dateTimeNow_int, 'sent' : False}
        update_data = {'lastDate':data['lastDate'], 'sent':data['sent']}
        if (activeCol.count_documents(query1) == 0):
            activeCol.update_many(query, {"$set": update_data})

    def reqHistoricalDataRemoveQueue(self, reqId, conId):
        db = Mongodb
        activeCol = self.db['ProcessQueue']
        if (reqId == None):
            query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'conId':conId, 'sent':True}
        else:
            query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'reqId':reqId, 'sent':True}
        activeCol.delete_many(query)
            

def main(argv):
    global runActive
    global QCount
    global QFunction
    
    try:
        opts, args = getopt.getopt(argv, "q:f:", ["queueCount=", "queueFunction="])
    except getopt.GetoptError:
        log.info("Missing Option -q value  or --queueCount=value")
        log.info("Missing Option -f value  or --queueFunction=[order / account / option]")
        
    for opt, arg in opts:
        print(opt, arg)
        if (opt == '-q'):
            QCount = arg
        if (opt == '-f'):
            QFunction = arg
    
    log.info("QueueCount: " + str(QCount))
    log.info("QueueFunction: " + QFunction)
    loop(10 * int(QCount))
    
    while True:
        Core_Load(QCount)
        while (runActive == True):
            loop(2)
        else:
            log.info("Starting Thread Termination and Reconnection Process")
            loop(60)
            runActive = True
            loop(30)
            log.info("Completing Thread Restart and Reconnection Process")


def loop(time):
    timeDelay = datetime.datetime.now() + datetime.timedelta(seconds=time)
    while (datetime.datetime.now() < timeDelay):
        pass


def Core_Load(processCount):
    app = IBApp()
    if (QFunction == "account" or QFunction == "option"):
        app.connect(mdTwsHost, twsPort, connectId + int(processCount))
    
    if (QFunction == "order"):
        app.connect(twsHost, twsPort, connectId + int(processCount))
    
    try:
        loop_core = threading.Thread(target=app.run)
        loop_core.daemon = True
        loop_core.start()
    except Exception as e:
        log.info("core initial run ERROR Captured")

    
if __name__ == "__main__":
    Rotation = RotatingFileHandler(filename='../logs/HedgeApplication_Queue.log', mode='a', maxBytes=20*1024*1024, backupCount=3, encoding=None, delay=0)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-25s %(levelname)-8s %(message)s', datefmt='%y-%m-%d %H:%M:%S', filename='../logs/HedgeApplication_Queue.log')
    log = logging.getLogger(__name__)
    log.addHandler(Rotation)
    log.addHandler(logging.NullHandler())
    log.addHandler(logging.StreamHandler(sys.stdout))
    
    log.info('Hedge Application Startup')
    
    try:
        main(sys.argv[1:])
    except Exception as e:
        log.info("main ERROR Captured " + str(e))
