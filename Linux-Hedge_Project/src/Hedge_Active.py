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
import multiprocessing as mp
import os
import numpy as np
import logging
import sys
import getopt
import configparser
import pymongo

random.seed(a=None, version=2)
event = threading.Event()
mp.set_start_method('spawn')

configParser = configparser.RawConfigParser()
configFilePath = '../../env/Env.conf'
configParser.read(configFilePath)

localHostAccount = configParser.get('ACCOUNT', 'IBKR_ACCT')
twsHost = configParser.get('ACCOUNT', 'TWSHOST')
twsPort = int(configParser.get('ACCOUNT', 'TWSPORT'))
connectId = int(configParser.get('ACCOUNT', 'CONNECTID'))
cycleTime = int(configParser.get('ACCOUNT', 'CYCLETIME'))
cycleDisTime = int(configParser.get('ACCOUNT', 'CYCLEDISTIME'))
threadThrottle = int(configParser.get('ACCOUNT', 'THREADTHROTTLE'))
reloadPositions = int(configParser.get('ACCOUNT', 'RELOADPOSITIONS'))

targetPnLTrigger = float(configParser.get('OPTION', 'PNLTRIGGER'))
targetPnLTriggerPer = float(configParser.get('OPTION', 'PNLTRIGGERPER'))
hedgePercentage = configParser.get('OPTION', 'HEDGEPERCENTAGE')
pt = configParser.get('OPTION', 'PROFITTARGET')
minExp = float(configParser.get('OPTION', 'MINEXPTIME'))
lookAheadDays = float(configParser.get('OPTION', 'LOOKAHEADDAYS'))
historyLoop = float(configParser.get('OPTION', 'HISTORYLOOP'))
historyReq = float(configParser.get('OPTION', 'HISTORYREQ'))
historyDelay = float(configParser.get('OPTION', 'HISTORYDELAY'))
optionLoadTime = float(configParser.get('OPTION', 'OPTIONLOADTIME'))


dbPath = configParser.get('DIRECTORY', 'DB')
logsPath = configParser.get('DIRECTORY', 'LOGS')
mongodbConn = configParser.get('DIRECTORY', 'MONGODBCONN')
mongoDB = configParser.get('DIRECTORY', 'MONGODB')

activeMode = configParser.get('MODE', 'ACTIVEMODE')
TestSwitch = configParser.get('MODE', 'TESTSWITCH')

hedgePercentage = (float(hedgePercentage)/100)
profitTarget = ((100 - int(pt))/100) 
OID = 0
seqCount = 0
netLiq = 0
buyPower = 0
runActive = True
activeHisCount = 0
threadCount = 0
ConCount = ""
ActiveFunction = ""
reqID_lock_Contracts = []
reqID_lock_AskBidOption = []
nextCryptoOrder = datetime.datetime.now()
PnLActive = False
targetBuyPower = 1000.00

SecStartTime = int(datetime.time(9,30,0).strftime("%H%M%S"))
SecEndTime = int(datetime.time(16,0,0).strftime("%H%M%S"))

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

    def nextValidId(self, orderId):
        if (orderId == 0):
            self.stop()
        else:
            log.info("nextValidId: " + str(orderId))
            global OID
            OID = orderId
            self.start()

# IBAPI Response One-time
    def securityDefinitionOptionParameter(self, reqId:int, exchange:str,
        underlyingConId:int, tradingClass:str, multiplier:str,
        expirations:SetOfString, strikes:SetOfFloat):
        print("SecurityDefinitionOptionParameter.", "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier, "Expirations:", expirations, "Strikes:", str(strikes),"\n")

    def securityDefinitionOptionParameterEnd(self, reqId:int):
        print("SecurityDefinitionOptionParameterEnd. ReqId:", reqId)
        
    def position(self, account, contract, position, avgCost):
        if (ActiveFunction == 'bpm') or (ActiveFunction == "position"):
            if (abs(position) < 100 and contract.secType == 'STK'):
                log.info("Position." + "Account: " + account + " Symbol: " + contract.symbol + " ConId " + str(contract.conId) + " SecType: " + contract.secType +  " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(position) + " Avg cost: " + str(avgCost) + " Right: " + contract.right + " Strike: " + str(contract.strike))
                DBApp.resAddAcctRecord(self, account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, position, avgCost, contract.right, contract.strike, contract.primaryExchange)
                if(ActiveFunction != "position"):
                    if (pos != 0):
                        self.reqAcctPnL(conId)
        elif(ActiveFunction == 'pnl') or (ActiveFunction == "position"):
            if (abs(position) >= 100) or (contract.secType == 'OPT') or (contract.secType == 'CRYPTO') or (position == 0):
                log.info("Position." + "Account: " + account + " Symbol: " + contract.symbol + " ConId " + str(contract.conId) + " SecType: " + contract.secType +  " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(position) + " Avg cost: " + str(avgCost) + " Right: " + contract.right + " Strike: " + str(contract.strike))
                DBApp.resAddAcctRecord(self, account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, position, avgCost, contract.right, contract.strike, contract.primaryExchange)
                if(ActiveFunction != "position"):
                    if (pos != 0):
                        self.reqAcctPnL(conId)

    def positionEnd(self):
        self.cancelPositions()
        log.info("Position Download End")
        
    def contractDetails(self, reqId, contractDetails):
        #log.info("contractDetails: ", reqId, " ", contractDetails, "\n")
        log.info("contractDetails: " + str(reqId) + " " + contractDetails.contract.symbol + " " + str(contractDetails.contract.conId) + " " + contractDetails.contract.secType + " " +  contractDetails.contract.right + " " + str(contractDetails.contract.strike))
        try:
            loop_contractDetailsStage = threading.Thread(target=self.contractDetailsStage, args=(reqId, contractDetails))
            loop_contractDetailsStage.daemon = True
            loop_contractDetailsStage.start()
            #self.contractDetailsStage(reqId, contractDetails)
        except Exception as e:
            log.info("contractDetails ERROR Captured " + str(e))

    def contractDetailsStage(self, reqId, contractDetails):
        #log.info("contractDetails: ", reqId, " ", contractDetails, "\n")
        try:
            if(reqId <= 50000):
                Mongodb.reqDBConHold(self, 'create')
                DBAppOption.resOptionsInfo(self, reqId, contractDetails.contract.symbol, contractDetails.contract.conId, contractDetails.contract.secType, contractDetails.contract.currency, contractDetails.contract.lastTradeDateOrContractMonth, contractDetails.contract.right, contractDetails.contract.strike, contractDetails.contract.primaryExchange)
            if (reqId > 50000):
                processQueue.reqHistoricalDataQueue(self, reqId, contractDetails.contract.symbol, contractDetails.contract.conId, contractDetails.contract.secType, 'Account')
        except Exception as e:
            log.info("contractDetailsStage ERROR Captured " + str(e))

    def contractDetailsEnd(self, reqId):
        log.info("contractDetails Download End")
        try:
            loop_contractDetailsEndStage = threading.Thread(target=self.contractDetailsEndStage(reqId))
            loop_contractDetailsEndStage.daemon = True
            loop_contractDetailsEndStage.start()
        except Exception as e:
            log.info("contractDetailsEnd ERROR Captured " + str(e))

    def contractDetailsEndStage(self, reqId):
        log.info("contractDetails Download End " + str(reqId))
        Mongodb.reqDBConHold(self, 'remove')
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
        global activeHisCount
        if(activeHisCount < 0):
            activeHisCount = 0
        if(reqId <= 50000):
            pass
        if(reqId > 50000):
            pass
        
# IBAPI Response Subscription
    def positionMulti(self, reqId: int, account: str, modelCode: str, contract: Contract, pos: int, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
        global runActive
        if (ActiveFunction == 'bpm') or (ActiveFunction == "position"):
            if (abs(pos) < 100 and contract.secType == 'STK') or (pos == 0):
                log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
                try:
                    loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                    loop_positionMultiStage.daemon = True
                    loop(1)
                    loop_positionMultiStage.start()
                except Exception as e:
                    log.info("Position Thread ERROR Captured " + str(e))
        if(ActiveFunction == 'pnl') or (ActiveFunction == "position"):
            if (abs(pos) >= 100) or (pos == 0) or (contract.secType == 'OPT') or (contract.secType == 'CRYPTO'):
                log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
                try:
                    loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                    loop_positionMultiStage.daemon = True
                    loop(1)
                    loop_positionMultiStage.start()
                except Exception as e:
                    log.info("Position Thread ERROR Captured " + str(e))

    def positionMultiStage(self, account, symbol, conId, secType, currency, lastTradeDateOrContractMonth, pos, avgCost, right, strike, exchange):
        DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, lastTradeDateOrContractMonth, pos, avgCost, right, strike, exchange)
        if(ActiveFunction != "position"):
            if (pos != 0):
                self.reqAcctPnL(conId)

    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        log.info("PositionMultiEnd. RequestId: " + str(reqId))

    def pnlSingle(self, reqId:int, pos:int, dailyPnL:float, UnrealizedPnL:float, realizedPnL:float, value:float):
        log.info("Daily PnL Single Subscription. ReqId: " + str(reqId) + " Position: " + str(pos) + " DailyPnL: " + str(dailyPnL) + " UnrealizedPnL: " + str(UnrealizedPnL) +  " RealizedPnL: " + str(realizedPnL) + " Value: " + str(value))
        global PnLActive
        PnLActive = True
        try:
            if (ActiveFunction == "pnl") or (ActiveFunction == "bpm"):
                loop_reqPnLStageAcctPnL = threading.Thread(target=self.reqPnLStageAcctPnL, args=(reqId, UnrealizedPnL, pos))
                loop_reqPnLStageAcctPnL.daemon = True
                loop_reqPnLStageAcctPnL.start()
        except Exception as e:
            log.info("PnLStage Thread ERROR Captured " + str(e))
            loop(10)
        
    def reqPnLStageAcctPnL(self, reqId, UnrealizedPnL, position):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Account PnL Active: " + str(TC))
        DBApp.resPnLUpdateAcctRecord(self, reqId, UnrealizedPnL, position)
        Mongodb.reqAskBidAcctRecord(self, reqId)
        Mongodb.reqStockPriceAcctRecord(self, reqId)
        log.info("Thread Count Account PnL Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def reqAskBidEval(self, reqId):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Ask/Bid Active: " + str(TC))
        Mongodb.reqAskBidAcctRecord(self, reqId)
        Mongodb.reqStockPriceAcctRecord(self, reqId)
        log.info("Thread Count Ask/Bid Completed: " + str(TC))
        threadCount = threadCount - 1
    
    def reqOptionSTKEval(self, reqId):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Option Stock Active: " + str(TC))
        Mongodb.reqHedgeStatusAcctRecord(self, reqId)
        DBLogic.logicSelectOptionTargets(self, reqId)
        #Mongodb.reqContractDownloadAcctRecord_Loop(self, reqId)
        log.info("Thread Count Option Stock Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def reqOptionContractEval(self, reqId):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Option Stock Active: " + str(TC))
        #Mongodb.reqHedgeStatusAcctRecord(self, reqId)
        #DBLogic.logicSelectOptionTargets(self, reqId)
        Mongodb.reqContractDownloadAcctRecord_Loop(self, reqId)
        log.info("Thread Count Option Stock Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def reqOptionEval(self, reqId):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Option Active: " + str(TC))
        Mongodb.reqOptionCloseAcctRecord(self, reqId)
        DBLogic.logic_evaluateOption_positionSize(self, reqId)
        log.info("Thread Count Option Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        log.info("HistoricalDataUpdate. ReqId: " + str(reqId) + " BarData. " + str(bar.high) + str(bar.low))
        
    def accountSummary(self, reqId:int, account:str, tags:str, value:str, currency:str):
        global netLiq
        global targetPnLTrigger
        global targetBuyPower
        global buyPower
        log.info("Account Calculation: " +  account + " : " + tags + " : " + value)
        if(tags == "NetLiquidationByCurrency"):
            log.info("For NetLiquidity Calculation: " +  account + " : " + tags + " : " + value)
            netLiq = float(value)
            targetPnLTrigger = -(netLiq * (targetPnLTriggerPer/100))
            targetBuyPower = round((netLiq * 0.01),2)
            log.info("targetPnLTrigger adjusted: " + str(targetPnLTrigger))
            log.info("targetBuyPower adjusted: " + str(targetBuyPower))
        if(tags == "BuyingPower"):
            log.info("Updated BuyPower Value: " +  account + " : " + tags + " : " + value)
            buyPower = float(value)
            
    def accountSummaryEnd(self, reqId:int):
        log.info("Account Summary End")
        
    def updateAccountValue(self, key, value, currency, accountName):
        print (key, value)
        
# IBAPI Order Response
    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.secType, '@', contract.exchange, ':', order.action, order.orderType, order.totalQuantity, orderState.status)
        #DBOrder.resOptionOrderStatus(self, contract.conId, order.orderId, orderState.status)
        
    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)       
        #print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled, "Remaining:", remaining, "AvgFillPrice:", avgFillPrice, "PermId:", permId, "ParentId:", parentId, "LastFillPrice:", lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld, "MktCapPrice:", mktCapPrice)
        #DBOrder.resOptionOrderStatus(self, 'None', orderId, status)

    def execDetails(self, reqId, contract, execution):
        log.info("Order Executed: " + str(reqId) + " " + contract.symbol + " " + contract.secType + " " + contract.currency  + " " + str(execution.execId) + " " + str(execution.orderId) + " " + str(execution.shares) + " " + str(execution.lastLiquidity))
        #if(execution.shares > 0):
            #DBOrder.resOptionOrderStatus(self, contract.conId, execution.orderId, 'Filled')
        
    def completedOrder(self, contract, order, orderState):
        log.info ("completedOrder Symbol: " + contract.symbol + " conId: " + str(contract.conId) + " OrderId: " + str(order.permId) + " OrderState: " + orderState.status)
        #DBOrder.resOptionOrderStatus(self, contract.conId, order.orderId, orderState.status)

    def openOrderEnd(self):
        log.info("openOrderEnd Update")

    def completedOrdersEnd(self):
        log.info("Completed Orders")

# IBAPI Request
    def getAcctPnL(self, reqId, conId):
        log.info("getAcctPnL- " + "ReqId: " + str(reqId) + " conId: " + str(conId))
        self.reqPnLSingle(reqId, localHostAccount, "", conId)
            
    def acct_Position(self):
        log.info("Get Account Positions Update")
        self.reqPositions()
        
    def getContractDetails_optionPrice(self, symbol, secType, currency, monthExp, realTimeNum):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = secType
        contract.exchange = "SMART"
        contract.currency = currency
        #contract.conId = "317033694"
        contract.lastTradeDateOrContractMonth = monthExp  # October 2020
        log.info("getContractDetails_optionPrice: " + "ReqId: " + str(realTimeNum) + " Contract: " + str(contract))
        self.reqContractDetails(realTimeNum, contract)
        
    def getContractDetails_stockPrice(self, symbol, currency, realTimeNum):
        realTimeNum = realTimeNum
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = currency
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
        loop(2)
        self.reqAllOpenOrders()
        
    def orderCancel(self, orderId):
        self.cancelOrder(orderId)

    def start(self):
        global seqCount
        seqCount = seqCount + 1 
        log.info ("Global Sequence Count: " + str(seqCount))
        
        try:
            Mongodb.reqDBConHold(self, 'remove')
            if (ActiveFunction == "pnl"):
                Mongodb.clearStatusAcctRecord(self, 'Account')
                Mongodb.clearHedgeDownloadAcctRecord(self, 'Account')
                Mongodb.clearOptionAskBidRecord(self, 'Account', {"$or":[{"secType" : "STK"},{"secType" : "OPT"}]}, {"$set": {"ask" : 0.00, "bid" : 0.00, "positionPrice" : 0.00, "AskBidActive": False}})
                Mongodb.clearOptionAskBidRecord(self, 'Option', {"secType" : "OPT"}, {"$set": {"request" : False}})
                Mongodb.clearExpiredContractsAcctRecord(self, 'Account')
                Mongodb.clearExpiredContractsOptionRecord(self, 'Account','Option')
                processQueue.clearProcessQueue(self)
                #Mongodb.updateDocumentField(self, 'Account', {}, {"exchange" : ""}, {"exchange" : ""})
            if (ActiveFunction == "pnl") or (ActiveFunction == "bpm") or (ActiveFunction == "batch"):
                self.reqAccountSummary(100001, "All", "$LEDGER")
                self.reqAccountSummary(100002, "All", "BuyingPower")
            if (ActiveFunction == "pnl") or (ActiveFunction == "bpm") or (ActiveFunction == "position"):
                self.reqPositionsMulti(100005, localHostAccount, "")
        except Exception as e:
            log.info("Initiation of Application Startup Functions " + str(e))
              
        try:
            if(ActiveFunction == "pnl") and (TestSwitch == True):
                loop_position = threading.Thread(target=self.positionLoad_Loop)
                loop_position.start()
        except Exception as e:
            log.info("option general load ERROR Captured " + str(e))

        try:
            if(ActiveFunction == "pnl") or (ActiveFunction == "bpm"):
                loop_positionPnLUpdate = threading.Thread(target=self.positionPnLUpdate_Loop)
                loop_positionPnLUpdate.start()
        except Exception as e:
            log.info("positionPnLUpdate ERROR Capture " + str(e))
            
        try:
            if(ActiveFunction == "position"):
                loop_positionUpdate = threading.Thread(target=self.positionUpdate_Loop)
                loop_positionUpdate.start()
        except Exception as e:
            log.info("positionUpdate ERROR Capture " + str(e))
            
        try:
            if(ActiveFunction == "batch"):
                loop_optionEval = threading.Thread(target=self.optionEval_Loop)
                loop_optionEval.start()
        except Exception as e:
            log.info("optionEval ERROR Capture " + str(e))
            
        try:
            if(ActiveFunction == "contract"):
                loop_optionEval = threading.Thread(target=self.optionDownloadEval_Loop)
                loop_optionEval.start()
        except Exception as e:
            log.info("optionEval ERROR Capture " + str(e))
            
        try:
            if(ActiveFunction == "batch"):
                loop_bpm = threading.Thread(target=self.bpm_Loop)
                loop_bpm.start()
        except Exception as e:
            log.info("bpm ERROR Capture " + str(e))

        try:
            loop_checkConnection = threading.Thread(target=self.connectStatus_Loop)
            loop_checkConnection.start()
        except Exception as e:
            log.info("connection check ERROR Capture " + str(e))

    def positionLoad_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
        while (runActive == True):
            if (datetime.datetime.now() > nextX) and (PnLActive == False):
                log.info("Option Data Loading")
                #Account Functions
                Mongodb.reqAskBidAcctRecord(self, None)
                Mongodb.reqStockPriceAcctRecord(self, None)
                #Option Functions moved to Batch
                #Mongodb.reqOptionCloseAcctRecord(self, None)
                #DBLogic.logic_evaluateOption_positionSize(self, None)
                #Mongodb.reqHedgeStatusAcctRecord(self, None)
                #DBLogic.logicSelectOptionTargets(self, None)
                #Mongodb.reqContractDownloadAcctRecord_Loop(self, None)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
                        
    def positionPnLUpdate_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime + 5)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Update PnL Loop")
                Mongodb.clearStaleAcctRecord(self, 'Account')
                loop(2)
                DBApp.reqSubReset(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
            
    def positionUpdate_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=reloadPositions + 10)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Update Positions Loop")
                self.cancelPositionsMulti(100005)
                loop(2)
                self.reqPositionsMulti(100005, localHostAccount, "")
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=reloadPositions)
            loop(1)
            
    def optionEval_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime + 15)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                if (Mongodb.reqDBConHold(self, 'check') == False):
                    log.info("Option Evaluation Loop")
                    DBLogic.logic_selectPositionsEval(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
            
    def optionDownloadEval_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=optionLoadTime + 20)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Contract Download Loop")
                DBLogic.logic_downloadOptionEval(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=optionLoadTime)
            loop(1)
            
    def bpm_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime + 25)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                if (Mongodb.reqDBConHold(self, 'check') == False):
                    log.info("bpm Evaluation Loop")
                    Mongodb.reqBPMClearOpenOrders(self)
                    loop(2)
                    Mongodb.reqBPMCloseAcctRecord(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
            
    def connectStatus_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
        global runActive
        global PnLActive
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Connect Check Loop / Adjust PnLActive Value")
                PnLActive = False
                if (self.isConnected() == False):
                    runActive = False
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
    
    def reqAcctPnL(self, conId):
        log.info("Request Positions PnL: " + str(conId))
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
        self.client = pymongo.MongoClient(mongodbConn)
        self.db = self.client[mongoDB]

    # General Functions
    def showCollections(self):
        print(self.db.list_collection_names())
        
    def insertDocument(self, collection, data):
        activeCol = self.db[collection]     
        activeCol.insert_one(data)
               
    def recordQuery(self, collection, query):
        record = []
        activeCol = self.db[collection]
        record = activeCol.find_one(query)
        return (record)
    
    def recordQueries(self, collection, query):
        record = []
        activeCol = self.db[collection]
        record = activeCol.find_all(query)
        return (record)
    
    def recordUpdate(self, collection, query, data):
        record = []
        activeCol = self.db[collection]
        activeCol.update_one(query, { "$set" : data })

    def recordUpdates(self, collection, query, data):
        activeCol = self.db[collection]
        activeCol.update_many( query, { "$set" : data })
        
    def updateAcctRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_one(query, update_data)
        
    def updateDocumentField(self, collection, query, data, update_data):
        log.info("Add a Document Field " + str(data) + " to Database " + collection)
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_many(query, update_data)

    #Hedge Project Specific
    def clearStatusAcctRecord(self, collection):
        activeCol = self.db[collection]
        query = {"status" : True}
        update_data = {"$set": {"position" : 0, "status" : False, "subPnL" : False, "subPnLRequest" : False, "unRealizedPnL": 0}}
        activeCol.update_many(query, update_data)
        log.info("Status Flag Cleared")
        
    def clearStaleAcctRecord(self, collection):
        activeCol = self.db[collection]
        query = {"avgCost": 0, "position": {"$ne": 0}}
        update_data = {"$set": {"position" : 0, "subPnL" : True, "subPnLRequest" : True}}
        activeCol.update_many(query, update_data)
        log.info("Stale Record Identified Ready to Clear")
    
    def clearHedgeDownloadAcctRecord(self, collection):
        activeCol = self.db[collection]
        query = {"$or": [{"hedge" : True},{"optionDownload" : True}]}
        update_data = {"$set": {"hedge" : False, "optionDownload" :  False, "optionDownloadActive" : False}}
        activeCol.update_many(query, update_data)
        log.info("hedge and optionDownload Flag Cleared")
    
    def clearOptionAskBidRecord(self, collection, query, update_data):
        activeCol = self.db[collection]
        activeCol.update_many(query, update_data)
        log.info("Account & Option ask and bid values Cleared")
        
    def clearExpiredContractsAcctRecord(self, collection):
        activeCol = self.db[collection]
        TDate_obj = datetime.datetime.now()
        query = {"expDate" : {"$ne": "None"}, "secType" : "OPT"}
        for r in activeCol.find(query):
            if(datetime.datetime.strptime(str(r.get('expDate')), '%Y-%m-%d 00:00:00') < TDate_obj):
                conId = r.get('conId')
                query1 = {"conId" : conId}
                activeCol.delete_one(query1)
                log.info("Account / Option Records to be remove: " + str(r))
        log.info("Expired Option Contracts cleared from Account Collections")
        
    def clearExpiredContractsOptionRecord(self, AcctCollection, OptionCollection):
        activeCol = self.db[AcctCollection]
        activeCol1 = self.db[OptionCollection]
        TDate_obj = datetime.datetime.now()
        query = {"expDate" : {"$ne": "None"}, "secType" : "OPT"}
        for r in activeCol1.find(query):
            if(datetime.datetime.strptime(str(r.get('expDate')), '%Y%m%d') < TDate_obj):
                conId = r.get('conId')
                symbol = r.get('symbol')
                query1 = {"conId" : conId}
                query2 = {"symbol" : symbol, "secType" : "STK"}
                update_data2 = {"$set": {"hedge" : False, "optionDownload" : False}}
                activeCol1.delete_one(query1)
                activeCol.update_one(query2, update_data2)
                log.info("Account / Option Records to be remove: " + str(r))
        log.info("Expired Option Contracts cleared from Option Collections")
        
    def addUniqueRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        if(self.recordQuery(collection, query) == None):
            activeCol.insert_one(data)
        else:
            update_data = {"$set": update_data}
            activeCol.update_one(query, update_data)
            
    def reqPnLAcctRecord(self, conId):
        activeCol = self.db['Account']
        query = {"subPnLRequest" : False, "status" : True,  "conId" : conId}
        for r in activeCol.find(query):
            log.info("Subscribe to PnL for position: " + r.get('symbol'))
            reqId = r.get('realTimeNum')
            new_reqId = DBLogic.logic_duplicateResolutionThread(self, 'Account', 'realTimeNum', reqId)
            activeCol.update_one({'conId' : conId}, {"$set":{'subPnLRequest' : True}})
            IBApp.getAcctPnL(self, new_reqId, conId)
            
    def reqPnlsubDisableAcctRecord(self, conId):
        activeCol = self.db['Account']
        query = {"subPnL" : True, "status" : False, "conId" : conId}
        for r in activeCol.find(query):
            log.info("unSubscribe PnL for Position: " + r.get('symbol'))
            reqId = r.get('realTimeNum') 
            activeCol.update_one({'conId' : conId}, {"$set":{'subPnL' : False,'subPnLRequest' : False}})
            IBApp.sub_stop(self, reqId) 
            
    def reqAskBidAcctRecord(self, reqId):
        activeCol = self.db['Account']

        if (reqId == None):
            query = { "status" : True}
        else:
            query = { "status" :  True, "realTimeNum" : reqId }
        
        for r in activeCol.find(query):
            symbol = r.get('symbol')
            conId = r.get('conId')
            secType = r.get('secType')
            reqId = r.get('realTimeNum')
            priceDate_str = r.get('priceDate')
            priceDate_obj = datetime.datetime.strptime(priceDate_str, '%Y%m%d %H%M%S%f')
            if (priceDate_obj < datetime.datetime.now()):
                log.info("Subscribe to AskBid for position: " + symbol)
                processQueue.reqHistoricalDataQueue(self, reqId, symbol, conId, secType, 'Account')
            
    def reqStockPriceAcctRecord(self, reqId):
        activeCol = self.db['Account']
        
        if (reqId == None):
            query = {"status" : True, "secType" : "OPT", "ask": {"$ne" : 0}, "bid" : {"$ne" : 0}}
        else:
            query = {"status" : True, "secType" : "OPT", "ask": {"$ne" : 0}, "bid" : {"$ne" : 0}, "realTimeNum" : reqId}
        
        for rr in activeCol.find(query):
            realTimeNum = (rr.get('realTimeNum')) + 50000
            symbol = rr.get('symbol')
            currency = rr.get('currency')
            IBApp.getContractDetails_stockPrice(self, symbol, currency, realTimeNum)

    def reqBPMCloseAcctRecord(self):
        activeCol = self.db['Account']
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        liqTimeStart = 150000
        liqTimeStop = 160000
        
        global buyPower
        global targetBuyPower
        
        if (buyPower >= targetBuyPower):
            return
        else:
            if (nowTime >= liqTimeStart and nowTime < liqTimeStop):
                query = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":0.00}}
            else:
                query = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":5.00}}
                
            log.info("Positions Identified for BPM Closure: " + str(activeCol.count_documents(query)))
            
            for rec in activeCol.find(query):
                symbol = rec.get('symbol')
                secType = rec.get('secType')
                right = rec.get('right')
                currency = rec.get('currency')
                strike = rec.get('strike')
                limitPosition = rec.get('position')
                olimitPosition = abs(rec.get('position'))
                conId = rec.get('conId')
                
                if(limitPosition > 0):
                    direction = 'SELL'
                    limitPrice = rec.get('ask')
                if(limitPosition < 0):
                    direction = 'BUY'
                    limitPrice = rec.get('bid')
                
                aRealTimeNum = rec.get('realTimeNum')
                avgCost = rec.get('avgCost')
                ask = rec.get('ask')
                bid = rec.get('bid')
                stockPrice = rec.get('stockPrice')
                positionPrice = rec.get('positionPrice')
                expDate = rec.get('expDate')
                limitPrice_mid = DBLogic.logic_midPrice_Calculation(ask, bid)
                exchange = rec.get('exchange')
                orderType = 'LMT'
                algoStrategy = 'Adaptive'
                startTime = int(datetime.time(11,0,0).strftime("%H%M%S"))
                endTime = SecEndTime
                cashQty = 0.00
                
                # Stock Profit BPM Exit
                log.info("Close Profitable Stock Position for BPM " + symbol)
                DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, olimitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)            
    
    def reqBPMClearOpenOrders(self):
        activeCol = self.db['Account']
        activeCol2 = self.db['ProcessQueue']
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        liqTimeStart = 153000
        liqTimeStop = 160000
        
        global buyPower
        global targetBuyPower
        
        query_openOrder = {'eventType':'Order', 'secType':'STK','OrderStatus':'open'}
        
        if (buyPower >= targetBuyPower):
            activeCol2.delete_many(query_openOrder)
        
        for r in activeCol2.find(query_openOrder):
            conId = r.get('conId')

            if (nowTime >= liqTimeStart and nowTime <= liqTimeStop): 
                query_acctStatus = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":0.00}, "conId":conId}
            else:
                query_acctStatus = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":5.00}, "conId":conId}
            
            query_order = {'eventType':'Order', 'secType':'STK','OrderStatus':'open', 'conId':conId}
            if (activeCol.count_documents(query_acctStatus) == 0):
                activeCol2.delete_many(query_order)
                
    def reqOptionCloseAcctRecord(self, reqId):
        activeCol = self.db['Account']
        activeCol2 = self.db['ProcessQueue']
        
        if(reqId == None):
            query = {"status" : True, "secType" : "OPT", "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice" : {"$ne": 0}, "stockPrice": {"$ne":0}}
        else:
            query = {"status" : True, "secType" : "OPT", "realTimeNum" : reqId, "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice" : {"$ne": 0}, "stockPrice": {"$ne":0}}
        
        for rec in activeCol.find(query):
            activeOrder = False
            symbol = rec.get('symbol')
            secType = rec.get('secType')
            right = rec.get('right')
            currency = rec.get('currency')
            strike = rec.get('strike')
            limitPrice = rec.get('bid')
            limitPosition = abs(rec.get('position'))
            conId = rec.get('conId')
            direction = 'BUY'
            aRealTimeNum = rec.get('realTimeNum')
            avgCost = rec.get('avgCost')
            ask = rec.get('ask')
            bid = rec.get('bid')
            stockPrice = rec.get('stockPrice')
            positionPrice = rec.get('positionPrice')
            expDate = rec.get('expDate')
            actionDate_7 = expDate - datetime.timedelta(days=7)
            actionDate_14 = expDate - datetime.timedelta(days=14)
            limitPrice_mid = DBLogic.logic_midPrice_Calculation(ask, bid)
            limitPrice_min = DBLogic.logic_minPrice_Calculation(limitPrice)
            exchange = rec.get('exchange')
            orderType = 'LMT'
            algoStrategy = 'Adaptive'
            startTime = SecStartTime
            endTime = SecEndTime
            cashQty = 0.00
            
            # Covered Option Exits
            if(Mongodb.reqCoveredOptions(self, symbol) == True):
                log.info("Evaluate Covered Options for Closure " + symbol +  " : " + str(positionPrice))
                # Profitable Exit for Covered Option Contracts moving against Price 
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*.50) > positionPrice):                      
                        log.info("Covered Profit Exit - 50% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                
                if (activeOrder == False) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*.50) > positionPrice):
                        log.info("Covered Profit Exit - 50% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                
                # Profitable Exit for Covered Option Contract that have achieved 90% profit
                if (activeOrder == False):
                    if (((avgCost/100)*0.10) > positionPrice):
                        log.info("Covered Profit Exit - 90% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                
                # Profitable Exit for Covered Option Contracts moving against Price within 7 Days of Expiration 
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*.80) > positionPrice):                      
                        log.info("Covered Profit Exit - 20% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*.80) > positionPrice):
                        log.info("Covered Profit Exit - 20% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                        
                # Profitable Exit for Covered Option Contract on day of Expiration
                if (activeOrder == False) & (datetime.datetime.today() > expDate):
                    if ((avgCost/100) > positionPrice):
                        log.info("Covered Profit Exit - Expiration Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                
                # Unprofitable Exit for Covered Option Contract with caps in places        
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*1.50) > positionPrice):
                        log.info("Covered Unprofitable Exit - 7 Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                        
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*1.50) > positionPrice):
                        log.info("Covered Unprofitable Exit - 7 Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                
                if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'C') & (stockPrice > strike):
                    if (avgCost/100 < positionPrice):
                        log.info("Covered Unprofitable Exit - Expiration Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                        
                if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'P') & (stockPrice < strike):
                    if (avgCost/100 < positionPrice):
                        log.info("Covered Unprofitable Exit - Expiration Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
            
            # Naked Option Exits if moving against price and profitable
            if(Mongodb.reqCoveredOptions(self, symbol) == False):
                log.info("Evaluate Naked Options for Closure " + symbol +  " : " + str(positionPrice))
                # Exit for Naked Options Contracts
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*0.50) > positionPrice):
                        log.info("Uncovered Profit Exit - 50% " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
    
                if (activeOrder == False) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*0.50) > positionPrice):
                        log.info("Uncovered Profit Exit - 50% " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                        
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*0.75) > positionPrice):
                        log.info("Uncovered Profitable Exit - 7 Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
                        
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*0.75) > positionPrice):
                        log.info("Uncovered Profitable Exit - 7 Day - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
    
                if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*1.00) > positionPrice):
                        log.info("Uncovered Unprofitable Exit - Expiration Day " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
    
                if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*1.00) > positionPrice):
                        log.info("Uncovered Unprofitable Exit - Expiration Day " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                        loop(1)
                        activeOrder = True
            
            #Remove Orders that are no longer valid due to price movement
            query_OrderCheck = {'eventType': 'Order', 'OrderStatus': 'open', 'conId': conId, 'direction': direction}            
            if (activeOrder == False and activeCol2.count_documents(query_OrderCheck) != 0):
                log.info("Removed Order for Position: " + symbol)
                activeCol2.delete_many(query_OrderCheck)

    def reqStandaloneOptions(self, symbol):
        activeCol = self.db['Account']
        count = 0
        query = {"symbol" : symbol, "secType" : "OPT"}
        if(activeCol.count_documents(query) == 1):
            return True
        else:
            return False
        
    def reqCoveredOptions(self, symbol):
        activeCol = self.db['Account']
        optionAvail = False
        stockAvail = False
        query1 = {"symbol" : symbol, "secType" : "OPT"}
        query2 = {"symbol" : symbol, "secType" : "STK", "$or": [{"position" : {"$gte" : 100}}, {"position" :{"$lte" : -100}}]}
        
        if (activeCol.count_documents(query1) > 0) and (activeCol.count_documents(query2) > 0):
            return True
        else:
            return False

    def reqHedgeStatusAcctRecord(self, reqId):
        activeCol = self.db['Account']
        
        if(reqId == None):
            query = {"status" : True, "secType" : "STK", "unRealizedPnL" : {"$lt" : targetPnLTrigger}}
        else:
            query = {"status" : True, "secType" : "STK", "realTimeNum" : reqId, "unRealizedPnL" : {"$lt" : targetPnLTrigger}}

        for r in activeCol.find(query):
            symbol = r.get('symbol')
            position = r.get('position')
            optionDownload = r.get('optionDownload')
            optionDownloadActive = r.get('optionDownloadActive')
            log.info("Evaluation of Hedge Status " + symbol)
            
            if(position >= 100):
                query1 = {"status" : True, "secType" : "OPT", "symbol" : symbol, "right" : "C"}
            if(position <= -100):
                query1 = {"status" : True, "secType" : "OPT", "symbol" : symbol, "right" : "P"}
            
            count = activeCol.count_documents(query1)
            
            if(count == 0):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position}
                data = {"hedge" : True}
                update_data = {"$set": {"hedge" : data['hedge']}}
                activeCol.update_one(query2, update_data)
                
            if(count != 0):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position}
                data = {"hedge" : False, "optionDownload" : False, "optionDownloadActive" : False}
                update_data = {"$set": {"hedge" : data['hedge'], "optionDownload" : data['optionDownload'], "optionDownloadActive" : data['optionDownloadActive']}}
                activeCol.update_one(query2, update_data)
                        
        if(reqId == None):
            query3 = {"status" : True, "secType" : "STK", "hedge" : True, "unRealizedPnL" : {"$gt" : targetPnLTrigger}}
        else:
            query3 = {"status" : True, "secType" : "STK", "realTimeNum" : reqId, "hedge" : True, "unRealizedPnL" : {"$gt" : targetPnLTrigger}}
        data3 = {"hedge" : False, "optionDownload" : False, "optionDownloadActive" : False}
        update_data3 = {"$set": {"hedge" : data3['hedge'], "optionDownload" : data3['optionDownload'], "optionDownloadActive" : data3['optionDownloadActive']}}
        activeCol.update_one(query3, update_data3)
    
    def reqContractDownloadAcctRecord_Loop(self, reqId):
        activeCol = self.db['Account']
        
        if(reqId == None):
            query = {"status" : True, "hedge" : True, "secType" : "STK", "optionDownloadActive" : False, "unRealizedPnL" : {"$lt" : targetPnLTrigger}}
        else:
            query = {"status" : True, "realTimeNum": reqId, "hedge" : True, "secType" : "STK",  "optionDownloadActive" : False, "unRealizedPnL" : {"$lt" : targetPnLTrigger}}
        
        if (activeCol.count_documents(query) != 0):
            try:
                loop_reqContractDownloadAcctRecord = threading.Thread(target=Mongodb.reqContractDownloadAcctRecord(self, reqId))
                loop_reqContractDownloadAcctRecord.daemon = True
                loop_reqContractDownloadAcctRecord.start()
            except Exception as e:
                log.info("reqContractDownloadAcctRecord ERROR Captured " + str(e))
    
    def reqContractDownloadAcctRecord(self, reqId):
        activeCol = self.db['Account']
        if(reqId == None):
            query = {"status" : True, "hedge" : True, "secType" : "STK", "optionDownload" : False, "optionDownloadActive" : False, "unRealizedPnL" : {"$lt" : targetPnLTrigger}}
        else:
            query = {"status" : True, "realTimeNum": reqId, "hedge" : True, "secType" : "STK", "optionDownload" : False, "optionDownloadActive" : False, "unRealizedPnL" : {"$lt" : targetPnLTrigger}}
        for r in activeCol.find(query):
            log.info("Contract Downloads for: " + r.get('symbol'))
            DBAppOption.reqOptionsInfo(self, r.get('symbol'), r.get('currency'), r.get('realTimeNum'))
            data = {"optionDownloadActive" : True, "AskBidActive" : False}
            update_data = {"$set": {"optionDownloadActive": data['optionDownloadActive'], "AskBidActive": data['AskBidActive']}}
            activeCol.update_one(query, update_data)
    
    def reqAskBidOptionRequestReset(self, reqId, right, priceDate_int, positionPrice_High, positionPrice_Low):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        activeCol2 = self.db['ProcessQueue']
        AcctRecord = []
        query_Acct = {"status" : True, "secType" : "STK", "hedge" : True, "positionPrice" : {"$ne":0}, "optionDownload" :  True, "realTimeNum" : reqId}
        AcctRecord = activeCol.find_one(query_Acct)
        
        query_Option = {"symbol" : AcctRecord.get('symbol'), "request" : True, "right" : right, "priceDate" : {"$lt": priceDate_int}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}
        query_ProcessQueue  = {"symbol" : AcctRecord.get('symbol'), "secType":"OPT" ,"$or": [{"sent" : False}, {"sent" : True}]}
        if (activeCol2.count_documents(query_ProcessQueue) == 0):
            if (activeCol1.count_documents(query_Option) != activeCol2.count_documents(query_ProcessQueue)):
                log.info("AskBid Option Reset in Progress for Symbol: " + AcctRecord.get('symbol'))
                activeCol1.update_many(query_Option, {"$set": {"request":False}})
    
    def reqAskBidOptionSelect(self, reqId):
        db = Mongodb()
        activeCol = self.db['Account']
        if (reqId == None):
            query = {"status" : True, "secType" : "STK", "hedge" : True, "positionPrice" : {"$ne":0}, "optionDownload" :  True}
        else:
            query = {"status" : True, "secType" : "STK", "hedge" : True, "positionPrice" : {"$ne":0}, "optionDownload" :  True, "realTimeNum" : reqId}
        for r in activeCol.find(query):
            symbol = r.get('symbol')
            position = r.get('position')
            positionPrice = r.get('positionPrice')
            if (position <= -100):
                right = "P"
            if (position >= 100):
                right = "C"                    
            log.info("Initiated download of " + symbol + " for getting ready to Trade")
            Mongodb.reqAskBidOptions(self, symbol, right, positionPrice)
            
    def reqAskBidOptions(self, symbol, right, positionPrice):
        db = Mongodb()
        activeCol = self.db['Option']
        dateTimeNow_obj = datetime.datetime.now() 
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        positionPrice_High = positionPrice + (positionPrice * hedgePercentage)
        positionPrice_Low = positionPrice - (positionPrice * hedgePercentage)
        
        query = {"symbol" :  symbol, "right" : right, "priceDate" : {"$lt": dateTimeNow_int}, "request" :  False, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}
        for rr in activeCol.find(query):
            reqId = rr.get('realTimeNum')
            conId = rr.get('conId')
            secType = rr.get('secType')
            log.info("Subscribe to Targeted AskBid for Option: " + symbol)
            DBLogic.logic_duplicateResolutionThread(self, 'Option', 'realTimeNum', reqId)
            processQueue.reqHistoricalDataQueue(self, reqId, symbol, conId, secType, 'Option')
            query1 = {"conId" : conId}
            data1 = {"request" : True}
            update_data1 = {"$set": {"request" : data1['request']}}
            activeCol.update_one(query1, update_data1)
        
    def reqDBConHold(self, mode):
        file = '/opt/local/env/DBConHold.tmp'
        
        if (mode == 'create') and (os.path.exists(file) ==  False):
            open(file, 'w').close()
        if (mode == 'remove') and (os.path.exists(file) ==  True):
            os.remove(file)
        if (mode == 'check'):
            if (os.path.exists(file) ==  True):
                return True
            if (os.path.exists(file) ==  False):
                return False
        
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
            update_data = {"position" : 0, "avgCost" : 0, "status" : data['status'], "priceDate" : data['priceDate'], "unRealizedPnL" : 0}
            db.addUniqueRecord('Account', query, data, update_data)
      
    def resPnLUpdateAcctRecord(self, reqId, unRealizedPnL, position):
        db = Mongodb()
        activeCol = self.db['Account']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        
        query_rec = {'realTimeNum':reqId}
        for rec in activeCol.find(query_rec):
            secType = rec.get('secType')
            avgCost = rec.get('avgCost')
            
            if (secType == 'OPT') and (position < 0):
                if (unRealizedPnL > 0):
                    c_value = (avgCost / (100)) - (abs(unRealizedPnL) / (abs(position) * 100))
                if (unRealizedPnL < 0):
                    c_value = (avgCost / (100)) + (abs(unRealizedPnL) / (abs(position) * 100))
            
            if (secType == 'STK'):
                if (position > 0):
                    c_value = avgCost + (unRealizedPnL / abs(position))
                if (position < 0):
                    c_value = avgCost - (unRealizedPnL / abs(position)) 
                
        query = {'realTimeNum' : reqId}
        data = {'unRealizedPnL' : round(unRealizedPnL,15), "position" : position, "ask" : round(c_value + .01,4), "bid" : round(c_value - .01,4), "positionPrice" : round(c_value,4), "priceDate" : priceDate_str}
        update_data = {"unRealizedPnL" : data['unRealizedPnL'], "subPnL" : True, "subPnLRequest" : True, "position" : data['position'], "ask" : data['ask'], "bid" : data['bid'], "positionPrice" : data['positionPrice'], "priceDate" : data['priceDate']}
        db.updateAcctRecord('Account', query, data, update_data)

    def resAskBidAcctRecord(self, reqId, ask, bid):
        db = Mongodb()
        activeCol = self.db['Account']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
                
        if(reqId <= 50000):
            query_rec = {'status':True, 'realTimeNum':reqId}
            for rec in activeCol.find(query_rec):
                position = rec.get('position')
                secType = rec.get('secType')
                avgCost = rec.get('avgCost')
                unRealizedPnL = rec.get('unRealizedPnL')
            
                if (secType == 'OPT'):
                    if (ask < 0 and bid < 0 and position < 0 and unRealizedPnL > 0 ):
                        c_value = (avgCost - unRealizedPnL) / (abs(position) * 100)
                        ask = round(abs(c_value),4)
                        bid = round(abs(c_value),4)
                        
                    PP = DBLogic.logic_midPrice_Calculation(ask, bid)
                    unRealizedPnL = (avgCost * abs(position)) - ((PP * 100) * abs(position))
                    
                if (secType == 'STK'):
                    PP = DBLogic.logic_midPrice_Calculation(ask, bid)
                    if (position > 0):
                        value = (PP - avgCost) * abs(position)
                        if (value != unRealizedPnL):
                            unRealizedPnL = value
                    if (position < 0):
                        value = (avgCost - PP) * abs(position)
                        if (value != unRealizedPnL):
                            unRealizedPnL = value

                positionPrice = PP
                DBApp.resAskBidAcctRecordCommit(self, reqId, ask, bid, positionPrice, unRealizedPnL)
            
        if(reqId > 50000):
            DBApp.resAskBidAcctRecordCommit(self, reqId, ask, bid, 0, 0)
        
    def resAskBidAcctRecordCommit(self, reqId, ask, bid, positionPrice, unRealizedPnL):
        db = Mongodb()
        activeCol = self.db['Account']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        
        if(reqId <= 50000):
            query = { "realTimeNum" : reqId }
            data = { "ask" :  ask, "bid" : bid, "priceDate" : priceDate_str, "positionPrice" : positionPrice, "unRealizedPnL" : unRealizedPnL }
            update_data = { "ask" :  data['ask'], "bid" : data['bid'], "priceDate" : data['priceDate'], "positionPrice" : data['positionPrice'], "unRealizedPnL" : data['unRealizedPnL'] }
            db.updateAcctRecord('Account', query, data, update_data)
        if(reqId > 50000):
            reqId = reqId - 50000
            stockPrice = DBLogic.logic_midPrice_Calculation(ask, bid)
            query = {"realTimeNum" : reqId}
            data = {"stockPrice" :  stockPrice}
            update_data = {"stockPrice" : data['stockPrice']}
            db.updateAcctRecord('Account', query, data, update_data)
            
    def reqSubReset(self):
        db = Mongodb()
        activeCol = self.db['Account']
        ActiveSubProcess = False

        if(ActiveFunction == "pnl"):
            query_positionSub = {"subPnL" : False, "subPnLRequest" : False, "$or": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}]}
        if(ActiveFunction == "bpm"):
            query_positionSub = {"subPnL" : False, "subPnLRequest" : False, "$nor": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}},{"position": 0}]}

        for r in activeCol.find(query_positionSub):
            ActiveSubProcess = True
            log.info("Add Subscription for PnL Contract: " + str(r.get('conId')))
            conId = r.get('conId')
            query_positionSub1 = {'conId':conId}
            activeCol.update_one(query_positionSub1, {"$set": {"status":True}})
            IBApp.reqAcctPnL(self, conId)
            
        query_positionDeSub = {"$or":[{"subPnL" : True},{"subPnLRequest" : True}], "position": 0}    

        for r in activeCol.find(query_positionDeSub):
            ActiveSubProcess = True
            log.info("Remove Subscription for PnL Contract: " + str(r.get('conId')))
            conId = r.get('conId')
            query_positionDeSub1 = {'conId':conId}
            activeCol.update_one(query_positionDeSub1, {"$set": {"status":False}})
            IBApp.reqAcctPnLdisable(self, conId)
        
        if (ActiveSubProcess == False):
            loop(15)
            if (ActiveFunction == "pnl"):
                query_subPnL = {"status": True, "$or": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "$or": [{"positionPrice" : { "$eq": 0 }},{"subPnL" : False}]}
                query_subPnLRequest = {"status": True, "$or": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "subPnL" : False, "subPnLRequest" : True}
            if (ActiveFunction == "bpm"):
                query_subPnL = {"status": True, "$nor": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "$or": [{"positionPrice" : { "$eq": 0 }},{"subPnL" : False}]}
                query_subPnLRequest = {"status": True, "$nor": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "subPnL" : False, "subPnLRequest" : True}
                
            if (activeCol.count_documents(query_subPnL) == activeCol.count_documents(query_subPnLRequest)):
                activeCol.update_many(query_subPnLRequest,{"$set":{"subPnLRequest" : False}})

class DBAppOption(IBApp):
    def reqOptionsInfo(self, symbol, currency,realTimeNum):
        if (processQueue.reqIDLock_Contracts(self, realTimeNum, 'lock') == True):
            monthExp = (datetime.datetime.today() + datetime.timedelta(days=lookAheadDays)).strftime("%Y%m")
            secType = 'OPT'
            log.info("Request Option Chain " + symbol)
            IBApp.getContractDetails_optionPrice(self, symbol, secType, currency, monthExp, realTimeNum)
            
    def resOptionsInfo(self, reqId, symbol, conId, secType, currency, expDate_obj, right, strike, exchange):
        db = Mongodb()
        RTNum = DBLogic.random_RTP(self)
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        
        if (exchange == "" and secType == "OPT"):
            exchange = "SMART"
        
        query = {'conId' : conId}
        data = {'symbol' : symbol, 'conId' : conId, 'secType' : secType, 'currency' : currency, 'expDate' : expDate_obj, 'right' : right, 'strike' : strike, 'exchange' : exchange,
                'ask' : 0.00, 'bid' : 0.00, 'positionPrice' : 0.00, 'priceDate' : priceDate_int, 'aRealTimeNum' :  reqId, 'realTimeNum' : RTNum, 'request' : False}
        update_data = {"request" : data['request']}
        db.addUniqueRecord('Option', query, data, update_data)
        
    def resOptionsInfoComplete(self, reqId):
        db = Mongodb()
        query = { "realTimeNum" : reqId, "secType" : "STK"}
        data =  { "optionDownloadActive": True, "optionDownload": True }
        update_data = {"optionDownloadActive" : data['optionDownloadActive'], "optionDownload" : data['optionDownload']}
        log.info("Contract Download Ended " + str(reqId))
        db.updateAcctRecord('Account', query, data, update_data)
        processQueue.reqIDLock_Contracts(self, reqId, 'unlock')

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
            query1 = {"symbol" :  symbol, "right" : right, "priceDate" : {"$lt": dateTimeNow_int}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}       
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
        query2 = { 'eventType':'Order', 'OrderNumId' : RTNum }
        
        while (activeCol.count_documents(query) > 0) or (activeCol1.count_documents(query1) > 0) or (activeCol2.count_documents(query2) > 0):
            RTNum = RTNum + 1
        return RTNum
    
    def find_nearest(array, value):
        if (len(array) == 0) or (value == 0):
            return 0
        else:
            array = np.asarray(array)
            idx = (np.abs(array - value)).argmin()
            return array[idx]
        
    def logicSelectOptionTargets(self, reqId):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        array = []
        
        if(reqId == None):
            query = {'status' : True, 'secType' : 'STK', 'hedge' :  True, 'optionDownload' : True}
        else:
            query = {'status' : True, 'secType' : 'STK', 'realTimeNum' : reqId, 'hedge' :  True, 'optionDownload' : True}
        for sr in activeCol.find(query):
            reqId = sr.get('realTimeNum')
            symbol = sr.get('symbol')
            position = sr.get('position')
            positionPrice = sr.get('positionPrice')
            avgCost = sr.get('avgCost')
            ask = sr.get('ask')
            bid = sr.get('bid')
            unRealizedPnL = sr.get('unRealizedPnL')
            
            positionPrice_High = positionPrice + (positionPrice * hedgePercentage)
            positionPrice_Low = positionPrice - (positionPrice * hedgePercentage)
            
            if (position <= -100):
                right = "P"
            if (position >= 100):
                right = "C"
                
            query1 = {"symbol" :  symbol, "right" : right, "priceDate" : {"$lt": priceDate_int}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}} 
            count = activeCol1.count_documents(query1)
            log.info("Evaluating " + symbol + " for Order Generation. Option Records to be updated " + str(count))
            
            if (count != 0):
                try:
                    Mongodb.reqAskBidOptionRequestReset(self, reqId, right, priceDate_int, positionPrice_High, positionPrice_Low)
                    loop_reqAskBidOptionSelect = threading.Thread(target=Mongodb.reqAskBidOptionSelect(self, reqId))
                    loop_reqAskBidOptionSelect.daemon = True
                    loop_reqAskBidOptionSelect.start()
                except Exception as e:
                    log.info("Request AskBid Option ERROR Captured " + str(e))
            
            if (count == 0):
                log.info("Phase 1 Order - Processing Order for " + symbol)
                
                if (DBOrder.req_optionOrder_cancelCount(self, symbol, 1) != 0):
                    hedgePercentageA = hedgePercentage - (DBOrder.req_optionOrder_cancelCount(self, symbol, 2)/100)
                else:
                    hedgePercentageA = hedgePercentage
                    
                if (hedgePercentageA == 0):
                    hedgePercentageA = 0.02
               
                if (position < 0) & ((positionPrice - (positionPrice * hedgePercentageA)) > avgCost):
                    avgCost = (positionPrice - (positionPrice * hedgePercentageA))
                if (position > 0) & ((positionPrice + (positionPrice * hedgePercentageA)) < avgCost):
                    avgCost = (positionPrice + (positionPrice * hedgePercentageA))
                    
                query2 = {"symbol" : symbol, "right" : right, "ask" : {"$gt" : 0.25}, "bid" : {"$gt" : 0.25}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}
                for rr in activeCol1.find(query2):
                    array.append(rr.get('strike'))
    
                nearest = DBLogic.find_nearest(array, avgCost)
                log.info("symbol: " + symbol + " Nearest: " + str(nearest) + " Position: " + str(position))
                DBLogic.logicSelectOptionOrders(self, symbol, nearest, position)
        
    def logicSelectOptionOrders(self, symbol, strike, position):
        log.info("Phase 2 Order")
        db = Mongodb()
        activeCol1 = self.db['Option']
        startTime = SecStartTime
        endTime = SecEndTime
        orderType = 'LMT'
        algoStrategy = 'Adaptive'
        cashQty = 0.00
            
        array = []
        limitPrice = 0
        
        if(position < 0):
            right = "P"
        if(position > 0):
            right = "C"
        
        query = {"symbol" : symbol, "strike" : strike, "right" : right, "ask" : {"$gt" : 0.25 }, "bid" : {"$gt" : 0.25 }}
        for r in activeCol1.find(query):
            array.append(r.get('bid'))
            limitPrice = np.max(array)
            
        if (limitPrice != 0):
            query1 = {"symbol" : symbol, "strike" : strike, "right" : right, "bid" : {"$eq" : limitPrice}}
            for rr in activeCol1.find(query1):
                limitPosition = math.floor(abs(position)/100)
                conId = rr.get('conId')
                secType = rr.get('secType')
                currency = rr.get('currency')
                ask = rr.get('ask')
                bid = rr.get('bid')
                limitPrice = rr.get('bid')
                limitPrice_mid = DBLogic.logic_midPrice_Calculation(ask, bid)
                aRealTimeNum = rr.get('aRealTimeNum')
                exchange = rr.get('exchange')
                direction = 'SELL'
                log.info("Add Record to OrderDB symbol: " + symbol + " secType: " + secType + " right: " + right + " strikePrice: " + str(strike) + " limitPrice: " + str(limitPrice) + " limitPosition: " + str(limitPosition) + " conId: " + str(conId) + " Direction: " + direction + " startTime: " + str(startTime) + " endTime: " + str(endTime)+  "  RTN: " + str(aRealTimeNum))
                DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)

    def logic_evaluateOption_positionSize(self, reqId):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        startTime = SecStartTime
        endTime = SecEndTime
        orderType = 'LMT'
        algoStrategy = 'Adaptive'
        cashQty = 0.00
        
        if(reqId == None):
            query = {"status" : True, "secType" : "STK", "hedge" : False}
        else:
            query = {"status" : True, "secType" : "STK", "hedge" : False, "realTimeNum" : reqId}
        for r in activeCol.find(query):
            symbol = r.get('symbol')
            position = r.get('position')
            sPositionBlock = math.floor(abs(position)/100)
            realTimeNum = r.get('realTimeNum')
            
            query1 = {"status" : True, "symbol" : symbol, "secType" : "OPT", "ask" : {"$gt" : 0.15 }, "bid" : {"$gt" : 0.15 }}
            for rr in activeCol.find(query1):
                oPosition = rr.get('position')
                oConId = rr.get('conId')
                oPositionBlock = abs(rr.get('position'))
                oSecType = rr.get('secType')
                oStrike = rr.get('strike')
                oAsk = rr.get('ask')
                oBid = rr.get('bid')
                oCurrency = rr.get('currency')
                oExchange = rr.get('exchange')
                limitPrice  = rr.get('bid')
                limitPrice_mid = DBLogic.logic_midPrice_Calculation(oAsk, oBid)
                direction = 'SELL'
                aRealTimeNum = realTimeNum
                log.info("Evaluate Option size for Stock Positions " + symbol)
                
                if (sPositionBlock > oPositionBlock):
                    if(position < 0):
                        right = "P"
                    if(position > 0):
                        right = "C"
                    limitPosition = (sPositionBlock - oPositionBlock)
                    
                    if(limitPosition != 0):
                        log.info("Add Record to OrderDB symbol: " + symbol + " secType: " + oSecType + " right: " + right + " strikePrice: " + str(oStrike) + " limitPrice: " + str(limitPrice) + " limitPosition: " + str(limitPosition) + " conId: " + str(oConId) + " Direction: " + direction + " startTime: " + str(startTime) + " endTime: " + str(endTime)+  "  RTN: " + str(aRealTimeNum))
                        DBOrder.reqOptionOrderDB(self, symbol, oSecType, right, oCurrency, oStrike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, oSecType), cashQty, limitPosition, oConId, direction, oExchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum)
                    
    def logic_midPrice_Calculation(ask, bid):
        return ((ask+bid)/2)
    
    def logic_minPrice_Calculation(priceValue):
        if (priceValue < 0.025):
            return 0.025
        else:
            return priceValue
        
    def logic_priceComp_Calculation(ask, bid, direction, secType):
        if (secType == 'STK'):
            if (direction == 'SELL'):
                return DBLogic.logic_minPrice_Calculation(ask)
            if (direction == 'BUY'):
                return DBLogic.logic_minPrice_Calculation(bid)
        if (secType == 'OPT'):
            ratio = (ask / (bid+ask)) - (bid / (bid+ask))
            mid = ((ask+bid)/2)
            if (direction == 'SELL'):
                if (ratio >= 0.10):             
                    return DBLogic.logic_minPrice_Calculation(bid)
                else:
                    return DBLogic.logic_minPrice_Calculation(ask)
            if (direction == 'BUY'):
                if (ratio >= 0.10):
                    return DBLogic.logic_minPrice_Calculation(mid)
                else:
                    return DBLogic.logic_minPrice_Calculation(bid)
    
    def logic_duplicateResolution(self, collection, indexField, reqId):
        try:
            loop_duplicate = threading.Thread(target=DBLogic.logic_duplicateResolutionThread(self, collection, indexField, reqId))
            loop_duplicate.start()
        except Exception as e:
            log.info("option general load ERROR Captured " + str(e))
    
    def logic_duplicateResolutionThread(self, collection, indexField, reqId):
        activeCol = self.db[collection]
        for r in activeCol.find({indexField : reqId}):
            query_duplicate = {indexField : r.get(indexField)}
            if (activeCol.count_documents(query_duplicate) > 1):
                log.info("Adjusting Record: " + str(r.get(indexField)))
                new_indexField_value = DBLogic.random_RTP(self)
                data_duplicate = {indexField : new_indexField_value}
                if (collection == "Account"):
                    update_duplicate = {"$set": {indexField : data_duplicate[indexField], "subPnL" : False, "subPnLRequest" : False}}
                    activeCol.update_one(query_duplicate, update_duplicate)
                    log.info ("de-duplication process completed with Change")
                    return new_indexField_value
                if (collection == "Option"):
                    update_duplicate = {"$set": {indexField : data_duplicate[indexField]}}
                    activeCol.update_one(query_duplicate, update_duplicate)
                    log.info ("de-duplication process completed with Change")
                    return new_indexField_value
            else:
                log.info ("de-duplication process completed with No Change")
                return reqId
            
    def logic_selectPositionsEval(self):
        activeCol = self.db['Account']
        query_Positions = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, "$or": [{'position': {"$lte": -100}, 'secType': "STK"},{'position': {"$gte": 100}, 'secType': "STK"}, {'secType': "OPT"}]}
        query_Positions_Refresh = {'status': True, 'subPnL': True, 'positionPrice': {"$eq": 0}}

        for rrr in activeCol.find(query_Positions_Refresh):
            log.info("Batch Ask/Bid for Option Ready Positions: " + rrr.get('symbol') + " secType: " + rrr.get('secType') + " reqId: " + str(rrr.get('realTimeNum')))
            try:
                loop_reqAskBidEval = threading.Thread(target=IBApp.reqAskBidEval, args=(self, rrr.get('realTimeNum')))
                loop_reqAskBidEval.daemon = True
                loop_reqAskBidEval.start()
                #IBApp.reqAskBidEval(self, rr.get('realTimeNum'))
            except Exception as e:
                log.info("reqAskBidEval ERROR Capture " + str(e)) 
        
        for rr in activeCol.find(query_Positions):
            log.info ("Starting Evaluation of existing Options: " + rr.get('symbol') + " secType: " + rr.get('secType') + " reqId: " + str(rr.get('realTimeNum')))
            try:
                loop_reqOptionEval = threading.Thread(target=IBApp.reqOptionEval, args=(self, rr.get('realTimeNum')))
                loop_reqOptionEval.daemon = True
                loop_reqOptionEval.start()
                #IBApp.reqOptionEval(self, r.get('realTimeNum'))
            except Exception as e:
                log.info("reqOptionEval ERROR Capture " + str(e))
                
        for r in activeCol.find(query_Positions):    
            log.info ("Starting Evaluation for new Options: " + r.get('symbol') + " secType: " + r.get('secType') + " reqId: " + str(r.get('realTimeNum')))
            try:
                loop_reqOptionSTKEval = threading.Thread(target=IBApp.reqOptionSTKEval, args=(self, r.get('realTimeNum')))
                loop_reqOptionSTKEval.daemon = True
                loop_reqOptionSTKEval.start()
                #IBApp.reqOptionSTKEval(self, r.get('realTimeNum'))
            except Exception as e:
                log.info("reqOptionSTKEval ERROR Capture " + str(e))
                
    def logic_downloadOptionEval(self):
        activeCol = self.db['Account']
        query_Positions = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, 'hedge': True, 'optionDownload': False, 'optionDownloadActive': False}
        query_Reset = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, 'hedge': True, 'optionDownload': False}
        query_Reset_Update = {"$set": {'optionDownloadActive': False}}
        global runActive
        
        if (activeCol.count_documents(query_Positions) != 0):
            for r in activeCol.find(query_Positions):
                log.info ("Starting Download of Options: " + r.get('symbol') + " secType: " + r.get('secType') + " reqId: " + str(r.get('realTimeNum')))
                try:
                    loop_reqOptionSTKEval = threading.Thread(target=IBApp.reqOptionContractEval, args=(self, r.get('realTimeNum')))
                    loop_reqOptionSTKEval.daemon = True
                    loop_reqOptionSTKEval.start()
                except Exception as e:
                    log.info("reqOptionSTKEval ERROR Capture " + str(e))
                loop(cycleTime)
        else:
            activeCol.update_many(query_Reset, query_Reset_Update)


class CryptoOrder(IBApp):
    def reqCryptoOrder(self):
        global nextCryptoOrder
        cyptoCycle = 7200
        secType = 'CRYPTO'
        right = None
        currency = 'USD'
        strike = None
        limitPrice = None
        cashQty = 25.00
        limitPosition = None
        conId = None
        direction = 'BUY'
        exchange = 'PAXOS'
        orderType = 'MKT'
        algoStrategy = 'Adaptive'
        startTime = 0000
        endTime = 2359
        
        if (buyPower > 5000) and (datetime.datetime.now() > nextCryptoOrder):
            log.info("Executing Crypto Buy Orders")
            DBOrder.reqCryptoOrderDB(self, 'BTC', secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, 100010)
            loop(2)
            DBOrder.reqCryptoOrderDB(self, 'ETH', secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, 100011)
            loop(2)
            DBOrder.reqCryptoOrderDB(self, 'BCH', secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, 100012)
            loop(2)
            DBOrder.reqCryptoOrderDB(self, 'LTC', secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, 100012)
            loop(2)
            nextCryptoOrder = datetime.datetime.now() + datetime.timedelta(seconds=cryptoCycle)
        

class DBOrder(IBApp):
    def reqOptionOrderDB(self, symbol, secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum):
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        log.info("Phase 3 - Order DB Create Direct for symbol: " + symbol + " Started")
        
        rDate_obj = date.today()
        rDate_str = datetime.datetime.strftime(rDate_obj, '%s')
        rDate_int = float(rDate_str)
        OrderNumId = 0000
        
        try:
            query = {"eventType" : 'Order', "conId" : conId, "OrderStatus" : {"$ne": 'Cancelled'}}
            data = {'eventType' : 'Order', 'symbol' : symbol, 'secType' : secType, 'right' : right, 'currency' : currency, 'strike' : strike, 'limitPrice' : limitPrice, 'cashQty' : cashQty, 'limitPosition' : limitPosition, 
                    'conId' : conId, 'direction' : direction, 'exchange' : exchange, 'orderType' : orderType, 'algoStrategy' : algoStrategy, 'startTime' : startTime, 'endTime' : endTime, 
                    'OrderNumId' : OrderNumId, 'OrderStatus' : "open", 'recDate' : rDate_int, 'aRealTimeNum' : aRealTimeNum}
            update_data = {'recDate' : data['recDate']}
            
            query_OrderStatus = {"eventType" : "Order", 'symbol' : symbol, 'direction':direction, "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}, {"OrderStatus" : {"$eq" : "Transmitted"}},{"OrderStatus" : {"$eq" : "Filled"}}]}
            if (activeCol.count_documents(query_OrderStatus) == 0):
                log.info("Cleared for processing of Order Record for: " + symbol)
                
                if (activeCol.count_documents(query) == 0):
                    activeCol.insert_one(data)
                    log.info("Phase 3 - Inserting new Order Record for: "  + symbol)
                else:
                    #activeCol.update_one(query, {"$set": update_data})
                    #log.info("Updated new Order Record for: "  + symbol + " recDate")    
                    query1 = {'eventType' :  'Order', 'conId' : conId, 'OrderStatus' : 'open', 'limitPrice' : {"$ne": limitPrice}}
                    update_data1 = {'recDate' : data['recDate'], 'limitPrice' : data['limitPrice']}
                    activeCol.update_one(query1, {"$set": update_data1})
                    log.info("Phase 3 - " + str(limitPrice) + " Updated Order Record for: "  + symbol)
            else:
                log.info("Phase 3 - Order DB Record Exist for: " + symbol + " Bypassed")
        except Exception as e:
            log.info("Order DB Record Create ERROR Captured " + str(e))
            
    def reqCryptoOrderDB(self, symbol, secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum):
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        log.info("Phase 3 - Order DB Create Direct for symbol: " + symbol + " Started")
        
        rDate_obj = date.today()
        rDate_str = datetime.datetime.strftime(rDate_obj, '%s')
        rDate_int = float(rDate_str)
        OrderNumId = 0000
        
        try:
            query = {"eventType" : 'Crypto Order', "conId" : conId, "OrderStatus" : {"$ne": 'Cancelled'}}
            data = {'eventType' : 'Crypto Order', 'symbol' : symbol, 'secType' : secType, 'right' : right, 'currency' : currency, 'strike' : strike, 'limitPrice' : limitPrice, 'cashQty' : cashQty, 'limitPosition' : limitPosition, 
                    'conId' : conId, 'direction' : direction, 'exchange' : exchange, 'orderType' : orderType, 'algoStrategy' : algoStrategy, 'startTime' : startTime, 'endTime' : endTime, 
                    'OrderNumId' : OrderNumId, 'OrderStatus' : "open", 'recDate' : rDate_int, 'aRealTimeNum' : aRealTimeNum}
            update_data = {'recDate' : data['recDate']}
            
            query_OrderStatus = {"eventType" : "Crypto Order", 'symbol' : symbol, "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}, {"OrderStatus" : {"$eq" : "Transmitted"}}]}
            if (activeCol.count_documents(query_OrderStatus) == 0):
                log.info("Cleared for processing of Order Record for: " + symbol)
                
                if (activeCol.count_documents(query) == 0):
                    activeCol.insert_one(data)
                    log.info("Phase 3 - Inserting new Order Record for: "  + symbol)
                else:
                    activeCol.update_one(query, {"$set": update_data})
                    log.info("Updated new Order Record for: "  + symbol + " recDate")    
                    query1 = {'eventType' :  'Crypto Order', 'conId' : conId, 'OrderStatus' : 'open', 'limitPrice' : {"$ne": limitPrice}}
                    update_data1 = {'recDate' : data['recDate'], 'limitPrice' : data['limitPrice']}
                    activeCol.update_one(query1, {"$set": update_data1})
                    log.info("Phase 3 - Updated Order Record for: "  + symbol)
            log.info("Phase 3 - Order DB Create Direct for symbol: " + symbol + " Completed")
        except Exception as e:
            log.info("Order DB Record Create ERROR Captured " + str(e))
    
    def disabled_resOptionOrderStatus(self, conId, orderId, status):
        log.info("Phase 5 Order Update")
        log.info("Phase 5 Order Update")
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        
        OrderTime_obj = datetime.datetime.now()
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        query = {'OrderNumId' :  orderId, 'OrderStatus':{"$ne": status}}
        data = {'OrderStatus' :  status, 'OrderTime' : OrderTime_int}
        update_data = {'OrderStatus' :  data['OrderStatus'], 'OrderTime' : data['OrderTime']}       
        activeCol.update_one(query, {"$set": update_data})

    def disabled_req_optionOrder_activeCancel(self):
        activeCol2 = self.db['ProcessQueue']
        TDate_obj = date.today()
        TDate_str = datetime.datetime.strftime(TDate_obj, '%s')
        TDate_int = float(TDate_str)
        
        query = {'eventType':'Order', "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}, {"OrderStatus" : {"$eq" : "open"}}]}
        query1 = {'eventType':'Order', "$or" : [{"recDate" : {"$lt" : TDate_int}}, {"OrderStatus" : {"$eq" : "Inactive"}}]}
        for r in activeCol2.find(query):
            log.info("Phase 6 Order")
            orderId = r.get('OrderNumId')
            IBApp.orderCancel(self, orderId)
        loop(5)
        activeCol2.delete_many(query1)
        
    def req_optionOrder_cancelCount(self, symbol, multiplier):
        activeCol = self.db['ProcessQueue']
        query = {'eventType':'Order', 'OrderStatus' : 'Cancelled', 'symbol' : symbol}
        i = 0
        i = activeCol.count_documents(query)
        return (i * multiplier)
                
    def disabled_OrderNum(self):
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        activeCol2 = self.db['ProcessQueue']
        global OID
        OrderNumId = OID
        
        query = { "realTimeNum" : OrderNumId }
        query1 = { "realTimeNum" : OrderNumId }
        query2 = { 'eventType':'Order', 'OrderNumId' : OrderNumId }
        
        while (activeCol.count_documents(query) > 0) or (activeCol1.count_documents(query1) > 0) or (activeCol2.count_documents(query2) > 0):
            OrderNumId = OrderNumId + 1
        OID = OrderNumId
        return OrderNumId


class processQueue(IBApp):
    def clearProcessQueue(self):
        db = Mongodb
        activeCol = self.db['ProcessQueue']
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'} ], "$or": [{'sent' : True}, {'sent' : False}]}
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
        activeCol = self.db['ProcessQueue']
        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        i = 0
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'sent' : False}
        data = {'lastDate' : dateTimeNow_int, 'sent': True}
        update_data = {"$set":{'lastDate':data['lastDate'], 'sent':data['sent']}}        
        for r in activeCol.find(query).sort('recDate', 1):
            IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'))
            query1 = {"reqId" : r.get('reqId')}
            activeCol.update_one(query1, update_data)
            i = i + 1
            loop(delay)
            if(i >= historyReq):
                break
            
    def reqHistoricalDataResetQueue(self):
        db = Mongodb
        activeCol = self.db['ProcessQueue']
        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'sent' : True}
        data = {'recDate' :  dateTimeNow_int, 'sent' : False}
        update_data = {'recDate':data['recDate'], 'sent':data['sent']}
        activeCol.update_many(query, {"$set": update_data})

    def reqHistoricalDataRemoveQueue(self, reqId, conId):
        db = Mongodb
        activeCol = self.db['ProcessQueue']
        if (reqId == None):
            query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'conId':conId, 'sent':True}
        else:
            query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'reqId':reqId, 'sent':True}
        activeCol.delete_one(query)
    
    def reqIDLock_Contracts(self, reqId, action):
        global reqID_lock_Contracts
        
        if (reqID_lock_Contracts.count(reqId) > 0) and (action == 'lock'):
            return False
        else:
            reqID_lock_Contracts.append(reqId)
            return True
        
        if (reqID_lock_Contracts.count(reqId) > 0 ) and (action == 'unlock'):
            reqID_lock_Contracts.remove(reqId)
            return True
        else:
            return False
    
    def disabled_reqIDLock_AskBidOption(self, symbol, action):
        global reqID_lock_AskBidOption
        
        if (reqID_lock_AskBidOption.count(symbol) > 0) and (action == 'lock'):
            return False
        else:
            reqID_lock_AskBidOption.append(symbol)
            return True
        
        if (reqID_lock_AskBidOption.count(symbol) > 0 ) and (action == 'unlock'):
            reqID_lock_AskBidOption.remove(symbol)
            return True
        else:
            return False
        

def main(argv):
    global runActive
    global ConCount
    global ActiveFunction
    
    try:
        opts, args = getopt.getopt(argv, "c:f:", ["ConCount=", "ActiveFunction="])
    except getopt.GetoptError:
        log.info("Missing Option -q value  or --ConCount=value")
        log.info("Missing Option -f value  or --ActvieFunction=[pnl / batch / bpm / position / contract]")
    
    for opt, arg in opts:
        print(opt, arg)
        if (opt == '-c'):
            ConCount = arg
        if (opt == '-f'):
            ActiveFunction = arg
    
    while True:
        Core_Load(ConCount)    
        while (runActive == True):
            loop(2)
        else:
            log.info("Starting Thread Termination and Reconnection Process")
            loop(60)
            runActive = True
            loop(60)
            log.info("Completing Thread Restart and Reconnection Process")


def loop(time):
    timeDelay = datetime.datetime.now() + datetime.timedelta(seconds=time)
    while (datetime.datetime.now() < timeDelay):
        pass


def Core_Load(processCount):
    app = IBApp()
    app.connect(twsHost, twsPort, connectId + int(processCount))
    
    try:
        loop_core = threading.Thread(target=app.run)
        loop_core.daemon = True
        loop_core.start()
    except Exception as e:
        log.info("core initial run ERROR Captured")

    
if __name__ == "__main__":
    Rotation = RotatingFileHandler(filename='../logs/HedgeApplication_Active.log', mode='a', maxBytes=20*1024*1024, backupCount=3, encoding=None, delay=0)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)-25s %(levelname)-8s %(message)s', datefmt='%y-%m-%d %H:%M:%S', filename='../logs/HedgeApplication_Active.log')
    log = logging.getLogger(__name__)
    log.addHandler(Rotation)
    log.addHandler(logging.NullHandler())
    log.addHandler(logging.StreamHandler(sys.stdout))
    
    log.info('Hedge Application Startup')
    
    try:
        main(sys.argv[1:])
    except Exception as e:
        log.info("main ERROR Captured " + str(e))
