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
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import numpy as np
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
import asyncio
import motor.motor_asyncio
import talib

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
cycleTimeB = int(configParser.get('ACCOUNT', 'CYCLETIMEB'))
cycleDisTime = int(configParser.get('ACCOUNT', 'CYCLEDISTIME'))
threadThrottle = int(configParser.get('ACCOUNT', 'THREADTHROTTLE'))
reloadPositions = int(configParser.get('ACCOUNT', 'RELOADPOSITIONS'))
reloadRunTime = int(configParser.get('ACCOUNT', 'RELOADRUNTIME'))
recylePnLTime = int(configParser.get('ACCOUNT', 'RECYCLEPNLTIME'))

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
optionLongPercentage = float(configParser.get('OPTION', 'OPTIONLONGPERCENTAGE'))

dbPath = configParser.get('DIRECTORY', 'DB')
logsPath = configParser.get('DIRECTORY', 'LOGS')
mongodbConn = configParser.get('DIRECTORY', 'MONGODBCONN')
mongoDB = configParser.get('DIRECTORY', 'MONGODB')

activeMode = configParser.get('MODE', 'ACTIVEMODE')
TestSwitch = configParser.get('MODE', 'TESTSWITCH')

influxdbToken = "dPFwrsBdQdgZ9O3wfznhh5paGLFPxywF5FBXTUSk_gXJ_W_7b27ECFo0LxuvT4QiLL0GUyglGsUvxO3P_HscZg=="
influxdbOrg = "LesterJohnInvestments"
influxdbBucket = "USMarketData"

hedgePercentage = (float(hedgePercentage)/100)
profitTarget = ((100 - int(pt))/100) 
OID = 0
seqCount = 0
netLiq = 0
buyPower = 0
acctRealizedPnL = 0
acctRealizedLossLimit = 0
runActive = True
activeHisCount = 0
threadCount = 0
ConCount = ""
ActiveFunction = ""
reqID_lock_Contracts = []
reqID_lock_AskBidOption = []
reqID_lock_positionMulti = []
nextCryptoOrder = datetime.datetime.now()
PnLActive = False
targetBuyPower = 1000.00
QT0 = False
QT1 = False
OrderActive = False
OrderActiveAcctUpdateLoop = False
Container = False

SecStartTime = int(datetime.time(9,30,0).strftime("%H%M%S"))
SecEndTime = int(datetime.time(16,0,0).strftime("%H%M%S"))


class IBApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        Mongodb.__init__(self)

    def error(self, reqId, errorCode, errorString):
        global activeHisCount
        global runActive
        global OID
        if reqId > -1:
            log.info ("Error: " + str(reqId) + " " + str(errorCode) + " " + errorString)
            if(errorCode == 162):
                activeHisCount = activeHisCount - 1
                if(reqId <= 50000):
                    DBApp.resAskBidAcctRecord(self, reqId, 0, -1.00, -1.00, -1.00, -1.00)
                    DBAppOption.resAskBidOptionRecord(self, reqId, -1.00, -1.00)
                if(reqId > 50000):
                    pass
                processQueue.reqHistoricalDataRemoveQueue(self, reqId, None)
            
            if(errorCode == 103) and (ActiveFunction == 'order') and (Container == False):
                loop (60)
                cmd = 'sudo systemctl restart hedgeo.service'
                os.system(cmd)
            if(errorCode == 504) and (ActiveFunction == 'accountHis') and (Container == False):
                loop (20)
                cmd = 'sudo systemctl restart hedgeq.service'
                os.system(cmd)
            if(errorCode == 504) and (ActiveFunction == 'optionHis') and (Container == False):
                loop (20)
                cmd = 'sudo systemctl restart hedgeq.service'
                os.system(cmd)
                
            if(errorCode == 103) and (ActiveFunction == 'order') and (Container == True):
                loop (60)
                sys.exit(2)
            if(errorCode == 504) and (ActiveFunction == 'accountHis') and (Container == True):
                loop (20)
                sys.exit(2)
            if(errorCode == 504) and (ActiveFunction == 'optionHis') and (Container == True):
                loop (20)
                sys.exit(2)
                
            if(errorCode == 322) and (ActiveFunction == "pnl" or ActiveFunction == "bpm" or ActiveFunction == "option"):
                self.cancelHistoricalData(reqId)
            if(errorCode == 322 and ActiveFunction == "order"):
                processQueue.clearProcessQueueOrder(self,reqId)
            if(errorCode == 504):
                runActive = False
                self.stop()
            if(errorCode == 103):
                runActive = False
                self.stop()
            if(errorCode == 106):
                OID = OID + 1

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
        log.info("Position." + "Account: " + account + " Symbol: " + contract.symbol + " ConId " + str(contract.conId) + " SecType: " + contract.secType +  " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(position) + " Avg cost: " + str(avgCost) + " Right: " + contract.right + " Strike: " + str(contract.strike))

    def positionEnd(self):
        log.info("Position Download End")
        
    def contractDetails(self, reqId, contractDetails):
        #log.info("contractDetails: ", reqId, " ", contractDetails, "\n")
        log.info("contractDetails: " + str(reqId) + " " + contractDetails.contract.symbol + " " + str(contractDetails.contract.conId) + " " + contractDetails.contract.secType + " " +  contractDetails.contract.right + " " + str(contractDetails.contract.strike))
        try:
            loop_contractDetailsStage = threading.Thread(target=self.contractDetailsStage, args=(reqId, contractDetails))
            loop_contractDetailsStage.daemon = True
            loop_contractDetailsStage.start()
        except Exception as e:
            log.info("contractDetails ERROR Captured " + str(e))

    def contractDetailsStage(self, reqId, contractDetails):
        #log.info("contractDetails: ", reqId, " ", contractDetails, "\n")
        try:
            if(reqId <= 50000):
                #Mongodb.reqDBConHold(self, 'create')
                DBAppOption.resOptionsInfo(self, reqId, contractDetails.contract.symbol, contractDetails.contract.conId, contractDetails.contract.secType, contractDetails.contract.currency, contractDetails.contract.lastTradeDateOrContractMonth, contractDetails.contract.right, contractDetails.contract.strike, contractDetails.contract.primaryExchange)
            if (reqId > 50000):
                processQueue.reqHistoricalDataQueue(self, reqId, contractDetails.contract.symbol, contractDetails.contract.conId, contractDetails.contract.secType, 'Account', '60 S')
        except Exception as e:
            log.info("contractDetailsStage ERROR Captured " + str(e))
            forceReset(self)

    def contractDetailsEnd(self, reqId):
        log.info("contractDetails Download End")
        try:
            loop_contractDetailsEndStage = threading.Thread(target=self.contractDetailsEndStage(reqId))
            loop_contractDetailsEndStage.daemon = True
            loop_contractDetailsEndStage.start()
        except Exception as e:
            log.info("contractDetailsEnd ERROR Captured " + str(e))

    def contractDetailsEndStage(self, reqId):
        try:
            log.info("contractDetails Download End " + str(reqId))
            #Mongodb.reqDBConHold(self, 'remove')
            DBAppOption.resOptionsInfoComplete(self, reqId)
        except Exception as e:
            log.info("contractDetailsEnd ERROR Captured " + str(e))

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
            DBApp.resAskBidAcctRecord(self, reqId, bar.date, bar.high, bar.low, bar.open, bar.close)
            DBAppOption.resAskBidOptionRecord(self, reqId, bar.high, bar.low)
        if (reqId > 50000):
            DBApp.resAskBidAcctRecord(self, reqId, bar.date, bar.high, bar.low, bar.open, bar.close)
    
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
        processQueue.reqHistoricalDataRemoveQueue(self, reqId, None)
        if (activeHisCount == 0):
            processQueue.reqHistoricalDataResetQueue(self)
        
    # IBAPI Response Subscription
    def positionMulti(self, reqId: int, account: str, modelCode: str, contract: Contract, pos: int, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
        global runActive
        if (ActiveFunction == 'bpm'):
            if (abs(pos) < 100 and contract.secType == 'STK') or (pos == 0 and contract.secType == 'STK'):
                log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
                try:
                    loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                    loop_positionMultiStage.daemon = True
                    loop_positionMultiStage.start()
                except Exception as e:
                    log.info("Position Thread ERROR Captured " + str(e))
                    loop(180)
        if(ActiveFunction == 'pnl'):
            if (abs(pos) >= 100 and contract.secType == 'STK') or (pos == 0 and contract.secType == 'STK'):
                log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
                try:
                    loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                    loop_positionMultiStage.daemon = True
                    loop_positionMultiStage.start()
                except Exception as e:
                    log.info("Position Thread ERROR Captured " + str(e))
                    loop(180)
        if(ActiveFunction == 'option'):
            if (contract.secType == 'OPT'):
                log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
                try:
                    loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                    loop_positionMultiStage.daemon = True
                    loop_positionMultiStage.start()
                except Exception as e:
                    log.info("Position Thread ERROR Captured " + str(e))
                    loop(180)
        if(ActiveFunction == "position"):
            log.info("PositionMulti. RequestId: " + str(reqId) + " Account: " + account + " ModelCode: " + modelCode + " Symbol: " + contract.symbol + " SecType: " + contract.secType + " Currency: " + contract.currency + " Exchange " + contract.primaryExchange + " Position: " + str(pos) + " AvgCost: " + str(avgCost))
            try:
                loop_positionMultiStage = threading.Thread(target=self.positionMultiStage, args=(account, contract.symbol, contract.conId, contract.secType, contract.currency, contract.lastTradeDateOrContractMonth, pos, avgCost, contract.right, contract.strike, contract.primaryExchange))
                loop_positionMultiStage.daemon = True
                loop_positionMultiStage.start()
            except Exception as e:
                log.info("Position Thread ERROR Captured " + str(e))
                loop(180)

    def positionMultiStage(self, account, symbol, conId, secType, currency, lastTradeDateOrContractMonth, pos, avgCost, right, strike, exchange):
        loop(3)
        DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, lastTradeDateOrContractMonth, pos, avgCost, right, strike, exchange)
        if (ActiveFunction == 'position'):
            pass
        else:
            self.reqAcctPnL(conId)
            
    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        if(ActiveFunction == "position"):
            Mongodb.clearStaleAcctRecord(self, 'Account')
        log.info("PositionMultiEnd. RequestId: " + str(reqId))

    def pnlSingle(self, reqId:int, pos:int, dailyPnL:float, UnrealizedPnL:float, realizedPnL:float, value:float):
        log.info("Daily PnL Single Subscription. ReqId: " + str(reqId) + " Position: " + str(pos) + " DailyPnL: " + str(round(dailyPnL,4)) + " UnrealizedPnL: " + str(round(UnrealizedPnL,4)) +  " RealizedPnL: " + str(round(realizedPnL,4)) + " Value: " + str(value))
        global PnLActive
        PnLActive = True
        try:
            if (ActiveFunction == "pnl") or (ActiveFunction == "bpm") or (ActiveFunction == "option"):
                loop_reqPnLStageAcctPnL = threading.Thread(target=self.reqPnLStageAcctPnL, args=(reqId, round(dailyPnL,4), round(UnrealizedPnL,4), pos))
                loop_reqPnLStageAcctPnL.daemon = True
                loop_reqPnLStageAcctPnL.start()
        except Exception as e:
            log.info("PnLStage Thread ERROR Captured " + str(e))
            loop(180)
        
    def reqPnLStageAcctPnL(self, reqId, dailyPnL, UnrealizedPnL, position):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Account PnL Active: " + str(TC))
        DBApp.resPnLUpdateAcctRecord(self, reqId, dailyPnL, UnrealizedPnL, position)
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
        Mongodb.reqLongOptionStatusAcctRecord(self, reqId)
        DBLogic.logicSelectHedgeOptionTargets(self, reqId)
        DBLogic.logicSelectLongOptionTargets(self, reqId)
        DBLogic.logic_evaluateOption_positionSize(self, reqId)
        log.info("Thread Count Option Stock Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def reqOptionContractEval(self, reqId):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Option Stock Active: " + str(TC))
        Mongodb.reqContractDownloadAcctRecord(self, reqId)
        log.info("Thread Count Option Stock Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def reqOptionEval(self, reqId):
        global threadCount
        threadCount = threadCount + 1
        TC = threadCount
        log.info("Thread Count Option Active: " + str(TC))
        DBApp.reqOptionCloseAcctRecord(self, reqId)
        DBApp.reqOptionExerciseOptionsRecord(self, reqId)
        log.info("Thread Count Option Completed: " + str(TC))
        threadCount = threadCount - 1
        
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        log.info("HistoricalDataUpdate. ReqId: " + str(reqId) + " BarData. " + str(bar.high) + str(bar.low))
        
    def accountSummary(self, reqId:int, account:str, tags:str, value:str, currency:str):
        global netLiq
        global targetPnLTrigger
        global targetBuyPower
        global buyPower
        global acctRealizedPnL
        global OrderActiveAcctUpdateLoop
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
            if(buyPower > targetBuyPower) and (ActiveFunction == "order") and (OrderActiveAcctUpdateLoop == True):
                self.orderStatusLoad_LoopRT()
        if(tags == "RealizedPnL"):
            log.info("Updated RealizedPnL Value: " + account + " : " + tags + " : " + value)
            acctRealizedPnL = float(value)
        if(ActiveFunction == "batch"):
            DBLogic.logic_acctRealizedLossLimit(0)
                        
    def accountSummaryEnd(self, reqId:int):
        log.info("Account Summary End")
        
    def updateAccountValue(self, key, value, currency, accountName):
        print (key, value)
        
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
        
    def getAskBid(self, reqId, symbol, conId, secType, duration):
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
        self.reqHistoricalData(reqId, contract, '', duration, "1 MIN", "MIDPOINT", 0, 1, False, [])

    def orderEntry(self, orderId, contract, order):
        self.placeOrder(orderId, contract, order)
        loop(2)
        self.reqAllOpenOrders()
        
    def orderCancel(self, orderId):
        self.cancelOrder(orderId)

    def orderExercise(self, orderId, contract, position):
            self.exerciseOptions(orderId, contract, 1, position, localHostAccount, 1)

    def start(self):
        global seqCount
        seqCount = seqCount + 1 
        log.info ("Global Sequence Count: " + str(seqCount))
        
        try:
            Mongodb.reqDBConHold(self, 'remove')
            if (ActiveFunction == "position"):
                Mongodb.clearStatusAcctRecord(self, 'Account')
                Mongodb.clearOptionDownloadAcctRecord(self, 'Account')
                Mongodb.clearOptionAskBidRecord(self, 'Account', {"$or":[{"secType" : "STK"},{"secType" : "OPT"}]}, {"$set": {"ask" : 0.00, "bid" : 0.00, "positionPrice" : 0.00, "AskBidActive": False}})
                Mongodb.clearOptionAskBidRecord(self, 'Option', {"secType" : "OPT"}, {"$set": {"request" : False}})
                Mongodb.clearExpiredContractsRecord(self, 'Account')
                Mongodb.clearExpiredContractsRecord(self, 'Option')
                processQueue.clearProcessQueue(self)
                #Mongodb.clearExpiredContractsOptionRecord(self, 'Account','Option')
                #Mongodb.updateDocumentField(self, 'Account', {}, {"exchange" : ""}, {"exchange" : ""})
            if (ActiveFunction == "accountHis"):
                processQueue.clearProcessQueueFlag(self)
            if (ActiveFunction == "pnl") or (ActiveFunction == "bpm") or (ActiveFunction == "option") or (ActiveFunction == "batch") or (ActiveFunction == "order") or (ActiveFunction == "acct"):
                self.reqAccountSummary(105001, "All", "$LEDGER")
                self.reqAccountSummary(105002, "All", "BuyingPower")
            if (ActiveFunction == "pnl"):
                self.reqPositionsMulti(105005, localHostAccount, "")
            if (ActiveFunction == "bpm"):
                self.reqPositionsMulti(105006, localHostAccount, "")
            if (ActiveFunction == "position"):
                self.reqPositionsMulti(105007, localHostAccount, "")
            if (ActiveFunction == "option"):
                self.reqPositionsMulti(105008, localHostAccount, "")
        except Exception as e:
            log.info("Initiation of Application Startup Functions " + str(e))
        
        try:
            if(ActiveFunction == "pnl") and (TestSwitch == True):
                loop_position = threading.Thread(target=self.positionLoad_Loop)
                loop_position.start()
        except Exception as e:
            log.info("option general load ERROR Captured " + str(e))

        try:
            if(ActiveFunction == "pnl") or (ActiveFunction == "bpm") or (ActiveFunction == "option"):
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
            if(ActiveFunction == "option"):
                loop_recyclePnLSub = threading.Thread(target=self.recyclePnLSub_Loop)
                loop_recyclePnLSub.start()
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
            if(ActiveFunction == "batch") and (activeMode == 'BPM'):
                loop_bpm = threading.Thread(target=self.bpm_Loop)
                loop_bpm.start()
        except Exception as e:
            log.info("bpm ERROR Capture " + str(e))
            
        try:
            if (ActiveFunction == "accountHis" or ActiveFunction == "optionHis"):
                loop_reqHistorical = threading.Thread(target=self.historical_Loop)
                loop_reqHistorical.start()
        except Exception as e:
            log.info("Historical Request ERROR Capture " + str(e))
            
        try:
            if (ActiveFunction == "order"):
                loop_orderStatusLoad = threading.Thread(target=self.orderStatusLoad_Loop)
                loop_orderStatusLoad.start()
                loop_orderRecycleLoad = threading.Thread(target=self.orderRecycleLoad_Loop)
                loop_orderRecycleLoad.start()
        except Exception as e:
            log.info("Account Order Status ERROR Captured " + str(e))

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
                #Mongodb.clearStaleAcctRecord(self, 'Account')
                #loop(2)
                DBApp.reqSubReset(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
            
    def positionUpdate_Loop(self):
        if (reloadPositions == 0):
            return
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=reloadPositions + 10)
        maxRunTime = datetime.datetime.now() + datetime.timedelta(seconds=reloadRunTime)
        while (runActive == True) and (datetime.datetime.now() < maxRunTime):
            if (datetime.datetime.now() > nextX):
                log.info("Update Positions Loop")
                self.cancelPositionsMulti(105007)
                loop(2)
                self.reqPositionsMulti(105007, localHostAccount, "")
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=reloadPositions)
            loop(1)
            
    def positionUpdateRT_Loop(self):
        self.cancelPositionsMulti(105007)
        loop(1)
        self.reqPositionsMulti(105007, localHostAccount, "")

    def recyclePnLSub_Loop(self):
        if (recylePnLTime == 0):
            return
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=recylePnLTime)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                DBApp.recyclePnLSub(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=recylePnLTime)
            loop(1)
            
    def optionEval_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTimeB + 15)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                if (Mongodb.reqDBConHold(self, 'check') == False):
                    log.info("Option Evaluation Loop")
                    DBLogic.logic_selectPositionsEval(self, 0)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTimeB)
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
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTimeB + 25)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                if (Mongodb.reqDBConHold(self, 'check') == False):
                    log.info("bpm Evaluation Loop")
                    DBApp.reqBPMClearOpenOrders(self)
                    loop(2)
                    DBApp.reqBPMCloseAcctRecord(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTimeB)
            loop(1)
            
    def historical_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=randrange(60))
        global runActive
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Historical Data Request Loop")
                processQueue.reqHistoricalDataFromQueue(self, historyDelay)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=historyLoop + randrange(60))
            loop(1)
            
    def orderRecycleLoad_Loop(self):
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
        while (runActive == True):
            if (datetime.datetime.now() > nextX):
                log.info("Order Management Loop")
                DBOrder.reqOrderActiveCancel(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime)
            loop(1)
    
    def orderStatusLoad_Loop(self):
        global OrderActive
        nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime * 3)
        while (runActive == True):
            if (datetime.datetime.now() > nextX and OrderActive == False):
                OrderActive = True
                log.info("Order Management Batch Loop")
                self.reqOpenOrders()
                loop(2)
                DBOrder.reqOptionOrderEval(self)
                loop(2)
                DBOrder.reqOptionOrderCreate(self)
                loop(2)
                DBOrder.reqOptionExerciseCreate(self)
                nextX = datetime.datetime.now() + datetime.timedelta(seconds=cycleTime * 3)
                OrderActive = False
            loop(1)
            
    def orderStatusLoad_LoopRT(self):
        global OrderActive
        log.info("Order Management RealTime Loop")
        if (OrderActive == False):
            OrderActive = True
            self.reqOpenOrders()
            loop(2)
            DBOrder.reqOptionOrderEval(self)
            loop(2)
            DBOrder.reqOptionOrderCreate(self)
            loop(2)
            DBOrder.reqOptionExerciseCreate(self)
            OrderActive = False
            
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
        self.client = pymongo.MongoClient(mongodbConn, connect=False, maxIdleTimeMS=5000)
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
        record = activeCol.find(query)
        return (record)
    
    def recordCount(self, collection, query):
        count = 0
        activeCol = self.db[collection]
        count = activeCol.count_documents(query)
        return count
    
    def recordUpdate(self, collection, query, data):
        activeCol = self.db[collection]
        activeCol.update_one(query, { "$set" : data })

    def recordUpdates(self, collection, query, data):
        activeCol = self.db[collection]
        activeCol.update_many( query, { "$set" : data })
        
    def updateAcctRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_one(query, update_data)
        
    def delAcctRecords(self, collection, query):
        activeCol = self.db[collection]
        activeCol.delete_many(query)

    def delAcctRecord(self, collection, query):
        activeCol = self.db[collection]
        activeCol.delete_one(query)
        
    def updateDocumentField(self, collection, query, data, update_data):
        log.info("Add a Document Field " + str(data) + " to Database " + collection)
        activeCol = self.db[collection]
        update_data = {"$set": update_data}
        activeCol.update_many(query, update_data)
    
    def addUniqueRecord(self, collection, query, data, update_data):
        activeCol = self.db[collection]
        if(self.recordCount(collection, query) == 0):
            activeCol.insert_one(data)
        else:
            update_data = {"$set": update_data}
            activeCol.update_one(query, update_data)

    #Hedge Project Specific Clear Functions
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
    
    def clearOptionDownloadAcctRecord(self, collection):
        activeCol = self.db[collection]
        query = {"$or": [{"hedge" : True},{"optionDownload" : True},{"longOptionActive" : {"$ne":"none"}}]}
        update_data = {"$set": {"hedge" : False, "optionDownload" :  False, "optionDownloadActive" : False, "longOptionActive" : "none"}}
        activeCol.update_many(query, update_data)
        log.info("hedge and optionDownload Flag Cleared")
    
    def clearOptionAskBidRecord(self, collection, query, update_data):
        activeCol = self.db[collection]
        activeCol.update_many(query, update_data)
        log.info("Account & Option ask and bid values Cleared")
        
    def disabled_clearExpiredContractsRecord(self, collection):
        activeCol = self.db[collection]
        TDate_obj = datetime.datetime.now()
        query = {"expDate": {"$ne":''}, "secType": 'OPT'}
        for r in activeCol.find(query):
            if(datetime.datetime.strptime(str(r.get('expDate')), '%Y-%m-%d %H:%M:%S') < TDate_obj):
                conId = r.get('conId')
                query1 = {"conId" : conId}
                activeCol.delete_one(query1)
                log.info("Account / Option Records to be remove: " + str(r))
        log.info("Expired Option Contracts cleared from Collections")
        
    def clearExpiredContractsRecord(self, collection):
        db = Mongodb()
        query = {"expDate": {"$ne":''}, "secType": 'OPT'}
        db.delAcctRecords(collection, query)
        log.info("Expired Option Contracts cleared from Collections")
        
    def disabled_clearExpiredContractsOptionRecord(self, AcctCollection, OptionCollection):
        activeCol = self.db[AcctCollection]
        activeCol1 = self.db[OptionCollection]
        TDate_obj = datetime.datetime.today()
        query = {"expDate": {"$ne":''}, "secType": 'OPT'}
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
        
    def clearExpiredContractsOptionRecord(self, AcctCollection, OptionCollection):
        activeCol = self.db[AcctCollection]
        activeCol1 = self.db[OptionCollection]
        TDate_obj = datetime.datetime.today()
        query = {"expDate": {"$ne":''}, "secType": 'OPT'}
        for r in activeCol1.find(query):
            conId = r.get('conId')
            query1 = {"conId" : conId}
            activeCol1.delete_one(query1)
            log.info("Account / Option Records to be remove: " + str(r))
        log.info("Expired Option Contracts cleared from Option Collections")
                    
    def reqPnLAcctRecord(self, conId):
        db = Mongodb()
        query = {"subPnLRequest" : False, "status" : True,  "conId" : conId}
        for r in db.recordQueries('Account', query):
            log.info("Subscribe to PnL for position: " + r.get('symbol'))
            reqId = r.get('realTimeNum')
            new_reqId = DBLogic.logic_duplicateResolutionThread(self, 'Account', 'realTimeNum', reqId)
            query_update = {'conId' : conId}
            data_update = {'subPnLRequest' : True}
            db.recordUpdate('Account', query_update, data_update)
            IBApp.getAcctPnL(self, new_reqId, conId)
            
    def reqPnlsubDisableAcctRecord(self, conId):
        db = Mongodb()
        query = {"subPnL" : True, "status" : False, "conId" : conId}
        for r in db.recordQueries('Account', query):
            log.info("unSubscribe PnL for Position: " + r.get('symbol'))
            reqId = r.get('realTimeNum')
            query_update = {'conId' : conId}
            data_update = {'subPnL' : False,'subPnLRequest' : False}
            db.recordUpdate('Account', query_update, data_update)
            IBApp.sub_stop(self, reqId)
            
    def reqAskBidAcctRecord(self, reqId):
        db = Mongodb()

        if (reqId == None):
            query = { "status" : True}
        else:
            query = { "status" :  True, "realTimeNum" : reqId }
        
        for r in db.recordQueries('Account', query):
            symbol = r.get('symbol')
            conId = r.get('conId')
            secType = r.get('secType')
            reqId = r.get('realTimeNum')
            priceDate_str = r.get('priceDate')
            priceDate_obj = datetime.datetime.strptime(priceDate_str, '%Y%m%d %H%M%S%f')
            if (priceDate_obj < datetime.datetime.now()):
                log.info("Subscribe to AskBid for position: " + symbol)
                processQueue.reqHistoricalDataQueue(self, reqId, symbol, conId, secType, 'Account', '60 S')
            
    def reqStockPriceAcctRecord(self, reqId):
        db = Mongodb()
        
        if (reqId == None):
            query = {"status" : True, "secType" : "OPT", "ask": {"$ne" : 0}, "bid" : {"$ne" : 0}}
        else:
            query = {"status" : True, "secType" : "OPT", "ask": {"$ne" : 0}, "bid" : {"$ne" : 0}, "realTimeNum" : reqId}
        
        for rr in db.recordQueries('Account', query):
            realTimeNum = (rr.get('realTimeNum')) + 50000
            symbol = rr.get('symbol')
            currency = rr.get('currency')
            IBApp.getContractDetails_stockPrice(self, symbol, currency, realTimeNum)
            
    def reqStandaloneOptions(self, symbol):
        db = Mongodb()
        count = 0
        query = {"symbol" : symbol, "secType" : "OPT"}
        if(db.recordCount('Account', query) == 1):
            return True
        else:
            return False
        
    def reqCoveredOptions(self, symbol):
        db = Mongodb()
        optionAvail = False
        stockAvail = False
        query1 = {"symbol" : symbol, "secType" : "OPT", "position" : {"$lt":0}}
        query2 = {"symbol" : symbol, "secType" : "STK", "$or": [{"position" : {"$gte" : 100}}, {"position" :{"$lte" : -100}}]}
        
        if (db.recordCount('Account', query1) > 0) and (db.recordCount('Account', query2) > 0):
            return True
        else:
            return False

    def reqHedgeStatusAcctRecord(self, reqId):
        activeCol = self.db['Account']
        stockTrigger = 80
                
        if(reqId == None):
            query = {"status" : True, "secType" : "STK", "unRealizedPnL" : {"$lt" : targetPnLTrigger}, "$nor": [{"position" : {"$lt":stockTrigger, "$gt": -stockTrigger}}]}
        else:
            query = {"status" : True, "secType" : "STK", "unRealizedPnL" : {"$lt" : targetPnLTrigger}, "$nor": [{"position" : {"$lt":stockTrigger, "$gt": -stockTrigger}}], "realTimeNum" : reqId}

        for r in activeCol.find(query):
            symbol = r.get('symbol')
            position = r.get('position')
            optionDownload = r.get('optionDownload')
            optionDownloadActive = r.get('optionDownloadActive')
            log.info("Evaluation of Hedge Status " + symbol)
            
            if(position >= stockTrigger):
                query1 = {"status" : True, "secType" : "OPT", "position" : {"$lt":0}, "symbol" : symbol, "right" : "C"}
            if(position <= -stockTrigger):
                query1 = {"status" : True, "secType" : "OPT", "position" : {"$lt":0}, "symbol" : symbol, "right" : "P"}
            
            if(activeCol.count_documents(query1) == 0):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position}
                data = {"hedge" : True}
                update_data = {"$set": {"hedge" : data['hedge']}}
                activeCol.update_one(query2, update_data)
                
            if(activeCol.count_documents(query1) != 0):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position}
                data = {"hedge" : False}
                update_data = {"$set": {"hedge" : data['hedge']}}
                activeCol.update_one(query2, update_data)
                        
        if(reqId == None):
            query3 = {"status" : True, "secType" : "STK", "hedge" : True, "$or": [{"unRealizedPnL" : {"$gt" : targetPnLTrigger}},{"position" : {"$lt" : stockTrigger, "$gt": -stockTrigger}}]}
        else:
            query3 = {"status" : True, "secType" : "STK", "realTimeNum" : reqId, "hedge" : True, "$or": [{"unRealizedPnL" : {"$gt" : targetPnLTrigger}},{"position" : {"$lt" : stockTrigger, "$gt": -stockTrigger}}]}
        
        data3 = {"hedge" : False}
        update_data3 = {"$set": {"hedge" : data3['hedge']}}
        activeCol.update_one(query3, update_data3)
        
    def reqLongOptionStatusAcctRecord(self, reqId):
        db = Mongodb()
        activeLong = False
        stockTrigger = 80
        
        if (acctRealizedPnL < (netLiq * 0.005)):
            return
        
        if(reqId == None):
            query = {"status" : True, "secType" : "STK", "unRealizedPnL" : {"$lt" : -10.00}}
        else:
            query = {"status" : True, "secType" : "STK", "realTimeNum" : reqId, "unRealizedPnL" : {"$lt" : -10.00}}

        for r in db.recordQueries('Account', query):
            symbol = r.get('symbol')
            secType = r.get('secType')
            position = r.get('position')
            avgCost = r.get('avgCost')
            positionPrice = r.get('positionPrice')
            optionDownload = r.get('optionDownload')
            optionDownloadActive = r.get('optionDownloadActive')
            dailyPnL = r.get('dailyPnL')
            log.info("Evaluation of Long Option Status " + symbol)          
            
            if(DBLogic.logic_dailyPercentageChange(secType, position, avgCost, dailyPnL) > (optionLongPercentage)):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position, "realTimeNum" : reqId}
                if(position < 0):
                    query1 = {"status" : True, "secType" : "OPT", "position" : {"$gt":0}, "symbol" : symbol, 'right' : 'P'}
                    if (db.recordCount('Account', query1) == 0) & (timeSeriesMetrics.EMA_1d(self, symbol, 'bid', positionPrice) == True):
                        activeLong = True
                        data = {"longOptionActive" : "longP"}
                        update_data2 = {"longOptionActive" : data['longOptionActive']}
                        db.recordUpdate('Account', query2, update_data2)
                if(position > 0):
                    query1 = {"status" : True, "secType" : "OPT", "position" : {"$gt":0}, "symbol" : symbol, 'right' : 'C'}
                    if (db.recordCount('Account', query1) == 0) & (timeSeriesMetrics.EMA_1d(self, symbol, 'ask', positionPrice) == True):
                        data = {"longOptionActive" : "longC"}
                        update_data2 = {"longOptionActive" : data['longOptionActive']}
                        db.recordUpdate('Account', query2, update_data2)
            
            if(DBLogic.logic_dailyPercentageChange(secType, position, avgCost, dailyPnL) < -(optionLongPercentage)):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position, "realTimeNum" : reqId}
                if(position < 0):
                    query1 = {"status" : True, "secType" : "OPT", "position" : {"$gt":0}, "symbol" : symbol, 'right' : 'C'}
                    if (db.recordCount('Account', query1) == 0) & (timeSeriesMetrics.EMA_1d(self, symbol, 'ask', positionPrice) == True):
                        activeLong = True
                        data = {"longOptionActive" : "longC"}
                        update_data2 = {"longOptionActive" : data['longOptionActive']}
                        db.recordUpdate('Account', query2, update_data2)
                if(position > 0):
                    query1 = {"status" : True, "secType" : "OPT", "position" : {"$gt":0}, "symbol" : symbol, 'right' : 'P'}
                    if (db.recordCount('Account', query1) == 0) & (timeSeriesMetrics.EMA_1d(self, symbol, 'bid', positionPrice) == True):
                        activeLong = True
                        data = {"longOptionActive" : "longP"}
                        update_data2 = {"longOptionActive" : data['longOptionActive']}
                        db.recordUpdate('Account', query2, update_data2)
                            
            if(activeLong == False):
                query2 = {"symbol" : symbol, "secType" : "STK", "position" : position}
                data = {"longOptionActive" : "none"}
                update_data2 = {"longOptionActive" : data['longOptionActive']}
                db.recordUpdate('Account', query2, update_data2)
                        
        if(reqId == None):
            query3 = {"status" : True, "secType" : "STK", "unRealizedPnL" : {"$gt" : -10.00}}
        else:
            query3 = {"status" : True, "secType" : "STK", "realTimeNum" : reqId, "unRealizedPnL" : {"$gt" : -10.00}}
        
        data3 = {"longOptionActive" : "none"}
        update_data3 = {"longOptionActive" : data3['longOptionActive']}
        db.recordUpdate('Account', query3, update_data3)
    
    def reqContractDownloadAcctRecord_Loop(self, reqId):
        activeCol = self.db['Account']
        
        query = {"status" : True, "realTimeNum": reqId}
        
        if (activeCol.count_documents(query) != 0):
            try:
                loop_reqContractDownloadAcctRecord = threading.Thread(target=Mongodb.reqContractDownloadAcctRecord(self, reqId))
                loop_reqContractDownloadAcctRecord.daemon = True
                loop_reqContractDownloadAcctRecord.start()
                
            except Exception as e:
                log.info("reqContractDownloadAcctRecord ERROR Captured " + str(e))
    
    def reqContractDownloadAcctRecord(self, reqId):
        db = Mongodb()
        query = {"status" : True, "realTimeNum": reqId}
        for r in db.recordQueries('Account', query):
            log.info("Contract Downloads for: " + r.get('symbol'))
            DBAppOption.reqOptionsInfo(self, r.get('symbol'), r.get('currency'), r.get('realTimeNum'))
            data = {"optionDownloadActive" : True, "AskBidActive" : False}
            update_data = {"optionDownloadActive": data['optionDownloadActive'], "AskBidActive": data['AskBidActive']}
            db.updateAcctRecord('Account', query, data, update_data)
    
    def reqAskBidOptionRequestReset(self, reqId, right, priceDate_int, positionPrice_High, positionPrice_Low):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        activeCol2 = self.db['ProcessQueue']
        AcctRecord = []
        query_Acct = {"status" : True, "secType" : "STK", "$or": [{"hedge" : True},{"longOptionActive":{"$ne":"none"}}], "positionPrice" : {"$ne":0}, "optionDownload" :  True, "realTimeNum" : reqId}
        AcctRecord = activeCol.find_one(query_Acct)
        
        query_Option = {"symbol" : AcctRecord.get('symbol'), "request" : True, "right" : right, "priceDate" : {"$lt": priceDate_int}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}
        query_ProcessQueue  = {"symbol" : AcctRecord.get('symbol'), "secType":"OPT" ,"$or": [{"sent" : False}, {"sent" : True}]}
        if (activeCol2.count_documents(query_ProcessQueue) == 0):
            if (activeCol1.count_documents(query_Option) != activeCol2.count_documents(query_ProcessQueue)):
                log.info("AskBid Option Reset in Progress for Symbol: " + AcctRecord.get('symbol'))
                activeCol1.update_many(query_Option, {"$set": {"request":False}})
    
    def reqAskBidOptionSelect(self, reqId, right, positionPrice_High, positionPrice_Low):
        db = Mongodb()
        if (reqId == None):
            query = {"status" : True, "secType" : "STK", "$or": [{"hedge":True},{"longOptionActive":{"$ne":"none"}}], "positionPrice" : {"$ne":0}, "optionDownload" :  True}
        else:
            query = {"status" : True, "secType" : "STK", "$or": [{"hedge":True},{"longOptionActive":{"$ne":"none"}}], "positionPrice" : {"$ne":0}, "optionDownload" :  True, "realTimeNum" : reqId}
        for r in db.recordQueries('Account', query):
            symbol = r.get('symbol')
            position = r.get('position')
            positionPrice = r.get('positionPrice')                  
            log.info("Initiated download of " + symbol + " for getting ready to Trade")
            Mongodb.reqAskBidOptions(self, symbol, right, positionPrice_High, positionPrice_Low)
            
    def reqAskBidOptions(self, symbol, right, positionPrice_High, positionPrice_Low):
        db = Mongodb()
        dateTimeNow_obj = datetime.datetime.now() 
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        query = {"symbol" :  symbol, "right" : right, "priceDate" : {"$lt": dateTimeNow_int}, "request" :  False, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}
        for rr in db.recordQueries('Option', query):
            reqId = rr.get('realTimeNum')
            conId = rr.get('conId')
            secType = rr.get('secType')
            log.info("Subscribe to Targeted AskBid for Option: " + symbol)
            DBLogic.logic_duplicateResolutionThread(self, 'Option', 'realTimeNum', reqId)
            processQueue.reqHistoricalDataQueue(self, reqId, symbol, conId, secType, 'Option', '60 S')
            query1 = {"conId" : conId}
            data1 = {"request" : True}
            update_data1 = {"request" : data1['request']}
            db.updateAcctRecord('Option', query1, data1, update_data1)
        
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


class influxdb(IBApp):
    def __init__(self):
        self.influxClient = InfluxDBClient(url="http://ubuntudb000:8086", token=influxdbToken, org=influxdbOrg)
        self.write_api = self.influxClient.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.influxClient.query_api()
        
    def pointWrite(self, measurement, tags, fieldName, field, barTime):
        point = Point(measurement) \
            .tag(tags[0],tags[1]) \
            .field(fieldName, field) \
            .time(barTime, WritePrecision.NS)
        self.write_api.write(influxdbBucket, influxdbOrg, point)
        
    def pointQuery(self, query):
        return self.query_api.query(org=influxdbOrg, query=query)


class timeSeriesMetrics(IBApp):
    def EMA_1d(self, symbol, metric, positionPrice):
        db = Mongodb()
        idb = influxdb()
        barMetric = []
        
        # metric = "ask" / "close" / "bid"
        
        query = 'from(bucket: "USMarketData") \
                    |> range(start: -365d) \
                    |> filter(fn: (r) => r["' + symbol + '"] == "' + metric + '") \
                    |> sort(columns:["_time"]) \
                    |> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'
        
        result = idb.pointQuery(query)
            
        results = []
        for table in result:
            for record in table.records:
                results.append((record.get_field(), record.get_value()))
    
        for field, val in results:
            barMetric.append(val)
            
        if (len(barMetric) < 60):
            query = {'status':  True, 'symbol': symbol, 'secType': 'STK'}
            for r in db.recordQueries('Account', query):
                symbol = r.get('symbol')
                conId = r.get('conId')
                secType = r.get('secType')
                reqId = r.get('realTimeNum')
                priceDate_str = r.get('priceDate')
                priceDate_obj = datetime.datetime.strptime(priceDate_str, '%Y%m%d %H%M%S%f')
                if (priceDate_obj < datetime.datetime.now()):
                    log.info("Subscribe to history for position: " + symbol)
                    query1 = {'conId': conId, 'duration': '3 M'}
                    if (db.recordCount('ProcessQueue', query1) == 0):
                        processQueue.reqHistoricalDataQueue(self, reqId, symbol, conId, secType, 'Account', '3 M')
                        return False
                    else:
                        return False

        barMetric = np.asarray(barMetric)
    
        output = talib.EMA(barMetric)

        if (metric == "ask"):
            if (output[len(output)-1] < positionPrice) & (output[len(output)-1] > output[len(output)-3] > output[len(output)-6] > output[len(output)-12]):
                return True
            else:
                return False

        if (metric == "bid"):
            if (output[len(output)-1] > positionPrice) & (output[len(output)-1] < output[len(output)-3] < output[len(output)-6] < output[len(output)-12]):
                return True
            else:
                return False


class DBApp(IBApp):
    def reqOptionCloseAcctRecord(self, reqId):
        global netLiq
        global acctRealizedPnL
        db = Mongodb()
         
        if(reqId == None):
            query = {"status" : True, "secType" : "OPT", "position" : {"$lt":0}, "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice" : {"$ne": 0}, "stockPrice": {"$ne":0}}
        else:
            query = {"status" : True, "secType" : "OPT", "position" : {"$lt":0}, "realTimeNum" : reqId, "ask":{"$gt":0}, "bid":{"$gt":0}, "positonPrice" : {"$ne": 0}, "stockPrice": {"$ne":0}}
        
        for rec in db.recordQueries('Account', query):
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
            unRealizedPnL = rec.get('unRealizedPnL')
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
            function = 'close'
            
            # Confirm BUY Option in Process
            query_LongClose = {'eventType': 'Order', 'OrderStatus': {"$ne":'open', "$ne":'Cancelled'}, 'conId': conId, 'direction': 'BUY'}
            if (db.recordCount('ProcessQueue', query_LongClose) != 0):
                return
            
            # Covered Option Exits for Covered
            if(Mongodb.reqCoveredOptions(self, symbol) == True) & (DBLogic.logic_acctRealizedLossLimit(unRealizedPnL) == True):
                log.info("Evaluate Covered Options for Closure " + symbol +  " : " + str(positionPrice))
                # Profitable Exit for Covered Option Contract that have achieved 90% profit
                if (activeOrder == False):
                    if (((avgCost/100)*0.10) > positionPrice):
                        log.info("Covered Profit Exit - 90% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                        
                if (activeOrder == False):
                    if (((avgCost/100)*0.20) > positionPrice) & (datetime.datetime.today() > actionDate_14):
                        log.info("Covered Profit Exit - 80% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                        
                if (activeOrder == False):
                    if (((avgCost/100)*0.30) > positionPrice) & (datetime.datetime.today() > actionDate_7):
                        log.info("Covered Profit Exit - 70% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                
                if (activeOrder == False):
                    if (((avgCost/100)*0.40) > positionPrice) & (buyPower == 0):
                        log.info("Covered Profit Exit - 80% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                
                # Profitable Exit for Covered Option Contracts moving against Price 
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100) * 0.90) > positionPrice) & (buyPower == 0):                      
                        log.info("Covered Profit Exit - 10% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                if (activeOrder == False) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100) * 0.90) > positionPrice) & (buyPower == 0):
                        log.info("Covered Profit Exit - 10% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True

                # Unprofitable Exit for Covered Option Contract with caps in places
                if (acctRealizedPnL > (netLiq * 0.007)):
                    if (activeOrder == False) & (datetime.datetime.today() > actionDate_14) & (right == 'C') & (stockPrice > strike):
                        if (((avgCost/100)*1.25) > positionPrice) & (DBLogic.logic_coverOptionStockCompare(self, symbol) == True):
                            log.info("Covered Unprofitable Exit - 14 Day - Multi " + symbol)
                            DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                            loop(1)
                            activeOrder = True
                    if (activeOrder == False) & (datetime.datetime.today() > actionDate_14) & (right == 'P') & (stockPrice < strike):
                        if (((avgCost/100)*1.25) > positionPrice) & (DBLogic.logic_coverOptionStockCompare(self, symbol) == True):
                            log.info("Covered Unprofitable Exit - 7 Day - Multi " + symbol)
                            DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                            loop(1)
                            activeOrder = True

                if (acctRealizedPnL > (netLiq * 0.005)):
                    if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'C') & (stockPrice > strike):
                        if (((avgCost/100)*1.50) > positionPrice) & (DBLogic.logic_coverOptionStockCompare(self, symbol) == True):
                            log.info("Covered Unprofitable Exit - 7 Day - Multi " + symbol)
                            DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                            loop(1)
                            activeOrder = True
                    if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (right == 'P') & (stockPrice < strike):
                        if (((avgCost/100)*1.50) > positionPrice) & (DBLogic.logic_coverOptionStockCompare(self, symbol) == True):
                            log.info("Covered Unprofitable Exit - 7 Day - Multi " + symbol)
                            DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                            loop(1)
                            activeOrder = True
                            
                if (acctRealizedPnL > (netLiq * 0.003)):
                    if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'C') & (stockPrice > strike):
                        if (((avgCost/100)*1.75) > positionPrice) & (DBLogic.logic_coverOptionStockCompare(self, symbol) == True):
                            log.info("Covered Unprofitable Exit - Expiration Day - Multi " + symbol)
                            DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                            loop(1)
                            activeOrder = True
                    if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'P') & (stockPrice < strike):
                        if (((avgCost/100)*1.75) > positionPrice) & (DBLogic.logic_coverOptionStockCompare(self, symbol) == True):
                            log.info("Covered Unprofitable Exit - Expiration Day - Multi " + symbol)
                            DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                            loop(1)
                            activeOrder = True
            
            # Naked Option Exits if moving against price and profitable
            if(Mongodb.reqCoveredOptions(self, symbol) == False) & (DBLogic.logic_acctRealizedLossLimit(unRealizedPnL) == True) & (buyPower == 0):
                log.info("Evaluate Naked Options for Closure " + symbol +  " : " + str(positionPrice))
                # Exit for Naked Options Contracts based on buyPower 0
                if (((avgCost/100)*0.10) > positionPrice):
                        log.info("Covered Profit Exit - 90% - Multi " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                
                # Exit for Naked Options Contract that move against position        
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100) * 0.90) > positionPrice):
                        log.info("Uncovered Profit Exit - 10% " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                if (activeOrder == False) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100) * 0.90) > positionPrice):
                        log.info("Uncovered Profit Exit - 10% " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True

                if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'C') & (stockPrice > strike):
                    if (((avgCost/100)*1.00) > positionPrice):
                        log.info("Uncovered Unprofitable Exit - Expiration Day " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                if (activeOrder == False) & (datetime.datetime.today() > expDate) & (right == 'P') & (stockPrice < strike):
                    if (((avgCost/100)*1.00) > positionPrice):
                        log.info("Uncovered Unprofitable Exit - Expiration Day " + symbol)
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
            
            #Remove Orders that are no longer valid due to price movement
            query_OrderCheck = {'eventType': 'Order', 'OrderStatus': 'open', 'conId': conId, 'direction': direction}            
            if (activeOrder == False and db.recordCount('ProcessQueue', query_OrderCheck) != 0):
                log.info("Removed Order for Position: " + symbol)
                db.delAcctRecords('ProcessQueue', query_OrderCheck)
                
    def reqOptionExerciseOptionsRecord(self, reqId):
        global netLiq
        global acctRealizedPnL
        db = Mongodb()
        
        if(reqId == None):
            query = {"status" : True, "secType" : "OPT", "position" : {"$gt":0}, "positionPrice" : {"$ne":0}, "stockPrice" : {"$ne":0}}
        else:
            query = {"status" : True, "secType" : "OPT", "position" : {"$gt":0}, "realTimeNum" : reqId, "positionPrice" : {"$ne":0}, "stockPrice" : {"$ne":0}}
        
        for rec in db.recordQueries('Account', query):
            activeOrder = False
            symbol = rec.get('symbol')
            secType = rec.get('secType')
            right = rec.get('right')
            currency = rec.get('currency')
            strike = rec.get('strike')
            limitPrice = rec.get('bid')
            limitPosition = abs(rec.get('position'))
            conId = rec.get('conId')
            aRealTimeNum = rec.get('realTimeNum')
            avgCost = rec.get('avgCost')
            ask = rec.get('ask')
            bid = rec.get('bid')
            unRealizedPnL = rec.get('unRealizedPnL')
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
            timeNow = int(datetime.datetime.now().strftime("%H%M%S"))
            unProfitTradeTime = int(datetime.time(11,0,0).strftime("%H%M%S"))
            exerciseStartTime = int(datetime.time(15,50,0).strftime("%H%M%S"))
            exerciseEndTime = int(datetime.time(17,25,0).strftime("%H%M%S"))
            startTime = SecStartTime
            endTime = SecEndTime
            cashQty = 0.00
            function = 'close'
            
            query_stock = {'status': True, 'symbol': symbol, 'secType': 'STK', 'position' : {"$ne": 0}, 'positionPrice' : {"$ne": 0}}
            query_stock_long = {'status': True, 'symbol': symbol, 'secType': 'STK', 'position' : {"$gt": 0}, 'positionPrice' : {"$ne": 0}}
            query_stock_short = {'status': True, 'symbol': symbol, 'secType': 'STK', 'position' : {"$lt": 0}, 'positionPrice' : {"$ne": 0}}

            log.info("Evaluate Long Options to Exercise " + symbol +  " : " + str(positionPrice))
            
            # Confirm Exercise or in Process
            query_Exercise = {'eventType': 'OrderExercise', 'OrderStatus': {"$ne":'open', "$ne":'Cancelled'}, 'conId': conId, 'direction': 'Exercise'}
            if (db.recordCount('ProcessQueue', query_Exercise) != 0):
                return
            
            # Confirm SELL Option in Process
            query_LongClose = {'eventType': 'Order', 'OrderStatus': {"$ne":'open', "$ne":'Cancelled'}, 'conId': conId, 'direction': 'SELL'}
            if (db.recordCount('ProcessQueue', query_LongClose) != 0):
                return
            
            # Exercise Option if Premium is covered
            if (datetime.datetime.today() < expDate):
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike + ((avgCost/abs(limitPosition))/100)) & (db.recordCount('Account', query_stock_short) == 0):
                    log.info("Exercise CALL Option: " + symbol)
                    direction = 'Exercise'
                    DBOrder.reqOptionExerciseOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, exerciseStartTime, exerciseEndTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
    
                if (activeOrder == False) & (right == 'P') & (stockPrice < strike - ((avgCost/abs(limitPosition))/100)) & (db.recordCount('Account', query_stock_long) == 0):
                    log.info("Exercise PUT Option: " + symbol)
                    direction = 'Exercise'
                    DBOrder.reqOptionExerciseOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, exerciseStartTime, exerciseEndTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True

            # Exercise In-the-Money on Expiration Date out of Conflict
            if (datetime.datetime.today() > expDate):
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike) & (db.recordCount('Account', query_stock_short) == 0):
                    log.info("Exercise CALL Option: " + symbol)
                    direction = 'Exercise'
                    DBOrder.reqOptionExerciseOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, exerciseStartTime, exerciseEndTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
                
                if (activeOrder == False) & (right == 'P') & (stockPrice < strike) & (db.recordCount('Account', query_stock_long) == 0):
                    log.info("Exercise PUT Option: " + symbol)
                    direction = 'Exercise'
                    DBOrder.reqOptionExerciseOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, exerciseStartTime, exerciseEndTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
                    
            #Remove Exercise Orders that are no longer valid due to price movement
            query_OrderCheck = {'eventType': 'OrderExercise', 'OrderStatus': 'open', 'conId': conId, 'direction': 'Exercise'}            
            if (activeOrder == False and db.recordCount('ProcessQueue', query_OrderCheck) != 0):
                log.info("Removed Long Order Exercise for Position: " + symbol)
                db.delAcctRecords('ProcessQueue', query_OrderCheck)

            # Sell Option based on Profit above 0
            if (datetime.datetime.today() < expDate) & (DBLogic.logic_acctRealizedLossLimit(unRealizedPnL) == True):
                if (activeOrder == False) & (acctRealizedPnL < 1000.00) & (unRealizedPnL > 10.00):
                    log.info("Close Long Option at < 10.00 " + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
                
                if (activeOrder == False) & (unRealizedPnL > (avgCost * 0.15)):
                    log.info("Close Long Option at 15% " + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
                
                if (activeOrder == False) & (datetime.datetime.today() > actionDate_14) & (unRealizedPnL > (avgCost * .10)):
                    log.info("Close Long Option at 10% 7-days to Expiration" + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True

                if (activeOrder == False) & (datetime.datetime.today() > actionDate_7) & (unRealizedPnL > (avgCost * .05)):
                    log.info("Close Long Option at 10% 7-days to Expiration" + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True    

                # Close long position that are out of money within 14-days of expiration with + acctRealizedPnL
                if (datetime.datetime.today() > actionDate_14) and (datetime.datetime.today() < expDate) and (unRealizedPnL < 0) and (acctRealizedPnL > (netLiq * 0.005)):
                    if (activeOrder == False) & (right == 'C') & (stockPrice < strike):
                        log.info("Close Long Out-of-Money Call Option " + symbol)
                        direction = 'SELL'
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
        
                    if (activeOrder == False) & (right == 'P') & (stockPrice > strike):
                        log.info("Close Long Out-of-Money PUT Option " + symbol)
                        direction = 'SELL'
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                
                # Close long position that are out of money within 14-days of expiration with buyPower == 0        
                if (datetime.datetime.today() > actionDate_14) and (datetime.datetime.today() < expDate) and (unRealizedPnL < 0) and (acctRealizedPnL > (netLiq * 0.003)) and (buyPower == 0):
                    if (activeOrder == False) & (right == 'C') & (stockPrice < strike):
                        log.info("Close Long Out-of-Money Call Option " + symbol)
                        direction = 'SELL'
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
        
                    if (activeOrder == False) & (right == 'P') & (stockPrice > strike):
                        log.info("Close Long Out-of-Money PUT Option " + symbol)
                        direction = 'SELL'
                        DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                        loop(1)
                        activeOrder = True
                
            #Expiration Date actions
            if (datetime.datetime.today() > expDate) & (DBLogic.logic_acctRealizedLossLimit(unRealizedPnL) == True):
                if (activeOrder == False) & (right == 'C') & (stockPrice < strike):
                    log.info("Close Long Out-of-Money Call Option " + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
    
                if (activeOrder == False) & (right == 'P') & (stockPrice > strike):
                    log.info("Close Long Out-of-Money PUT Option " + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
                
            # Close In-the-Money on Expiration Date in Conflict
            if (datetime.datetime.today() > expDate) & (DBLogic.logic_acctRealizedLossLimit(unRealizedPnL) == True):
                if (activeOrder == False) & (right == 'C') & (stockPrice > strike) & (db.recordCount('Account', query_stock_short) != 0):
                    log.info("Close Long In-Money Call Option in conflict " + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True

                if (activeOrder == False) & (right == 'P') & (stockPrice < strike) & (db.recordCount('Account', query_stock_long) != 0):
                    log.info("Close Long In-Money PUT Option in conflict " + symbol)
                    direction = 'SELL'
                    DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    loop(1)
                    activeOrder = True
                
            #Remove Orders that are no longer valid due to price movement
            query_OrderCheck = {'eventType': 'Order', 'OrderStatus': 'open', 'conId': conId, 'direction': 'SELL'}            
            if (activeOrder == False and db.recordCount('ProcessQueue', query_OrderCheck) != 0):
                log.info("Removed Long Order SELL for Position: " + symbol)
                db.delAcctRecords('ProcessQueue', query_OrderCheck)
    
    def reqBPMCloseAcctRecord(self):
        db = Mongodb()
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        liqTimeStart = int(datetime.time(15,30,0).strftime("%H%M%S"))
        liqTimeStop = SecEndTime
        
        global buyPower
        global targetBuyPower
        
        if (buyPower >= targetBuyPower):
            return
        else:
            if (nowTime >= liqTimeStart and nowTime < liqTimeStop):
                query = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "position":{"$ne": 0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":1.00}}
            else:
                query = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "position":{"$ne": 0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":5.00}}
                
            log.info("Positions Identified for BPM Closure: " + str(db.recordCount('Account', query)))
            
            for rec in db.recordQueries('Account', query):
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
                function = 'close'
                
                # Stock Profit BPM Exit
                log.info("Close Profitable Stock Position for BPM " + symbol)
                DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, olimitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
    
    def reqBPMClearOpenOrders(self):
        db = Mongodb()
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        liqTimeStart = 153000
        liqTimeStop = 160000
        
        global buyPower
        global targetBuyPower
        
        query_openOrder = {'eventType':'Order', 'secType':'STK','OrderStatus':'open'}
        
        if (buyPower >= targetBuyPower):
            db.delAcctRecords('ProcessQueue', query_openOrder)
        
        for r in db.recordQueries('ProcessQueue', query_openOrder):
            conId = r.get('conId')

            if (nowTime >= liqTimeStart and nowTime <= liqTimeStop): 
                query_acctStatus = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "position":{"$ne": 0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":1.00}, "conId":conId}
            else:
                query_acctStatus = {"status":True, "subPnL":True, "secType":"STK", "ask":{"$gt":0}, "bid":{"$gt":0}, "position":{"$ne": 0}, "positonPrice":{"$ne": 0}, "unRealizedPnL":{"$gt":5.00}, "conId":conId}
            
            query_order = {'eventType':'Order', 'secType':'STK','OrderStatus':'open', 'conId':conId}
            if (db.recordCount('Account', query_acctStatus) == 0):
                db.delAcctRecords('ProcessQueue', query_order)
    
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
                'hedge' : False, 'optionDownload' : False, 'optionDownloadActive' : False, 'AskBidActive' : False, 'longOptionActive' : 'none',
                'recDate' : TDate_str, 'realTimeNum' : RTNum, 'status' : True }
        if (position != 0):
            update_data = {"account" : data['account'], "position" : data['position'], "avgCost" : data['avgCost'], "expDate" : data['expDate'], "status" : True, "priceDate" : data['priceDate']}
            db.addUniqueRecord('Account', query, data, update_data)
        if (position == 0):
            update_data = {"account" : data['account'], "position" : 0, "avgCost" : 0, "status" : False, "priceDate" : data['priceDate'], "longOptionActive" : "none", "unRealizedPnL" : 0}
            db.addUniqueRecord('Account', query, data, update_data)
      
    def resPnLUpdateAcctRecord(self, reqId, dailyPnL, unRealizedPnL, position):
        db = Mongodb()
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        c_value = 0
        
        query_rec = {'realTimeNum':reqId}
        for rec in db.recordQueries('Account', query_rec):
            secType = rec.get('secType')
            avgCost = rec.get('avgCost')
            
            if (secType == 'OPT'):
                if (position > 0):
                    c_value = (abs(avgCost * position) + unRealizedPnL) / abs(position * 100)
                if (position < 0):
                    c_value = (abs(avgCost * position) - unRealizedPnL) / abs(position * 100)
            
            if (secType == 'STK'):
                if (position > 0):
                    c_value = avgCost + (unRealizedPnL / abs(position))
                if (position < 0):
                    c_value = avgCost - (unRealizedPnL / abs(position)) 
            
            if (c_value == 0):
                return
            else:
                c_value = abs(c_value)
            
            query = {'realTimeNum' : reqId}
            data = {'unRealizedPnL' : round(unRealizedPnL,15), "position" : position, "ask" : round(c_value + .01,4), "bid" : round(c_value - .01,4), "positionPrice" : round(c_value,4), "priceDate" : priceDate_str, "dailyPnL" : dailyPnL}
            update_data = {"unRealizedPnL" : data['unRealizedPnL'], "subPnL" : True, "subPnLRequest" : True, "position" : data['position'], "ask" : data['ask'], "bid" : data['bid'], "positionPrice" : data['positionPrice'], "priceDate" : data['priceDate'], "dailyPnL" : data['dailyPnL']}
            db.updateAcctRecord('Account', query, data, update_data)

    def resAskBidAcctRecord(self, reqId, date, ask, bid, open, close):
        db = Mongodb()
        idb = influxdb()
        activeCol = self.db['Account']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%Y%m%d %H%M%S%f')
        
        if (date != 0):
            barTime = datetime.datetime.strptime(date, '%Y%m%d %H:%M:%S')

        if(reqId <= 50000):
            query_rec = {'status':True, 'realTimeNum':reqId}
            for rec in activeCol.find(query_rec):
                symbol = rec.get('symbol')
                position = rec.get('position')
                secType = rec.get('secType')
                avgCost = rec.get('avgCost')
                conId = rec.get('conId')
                unRealizedPnL = rec.get('unRealizedPnL')
            
                if (secType == 'OPT'):
                    if (ask < 0 and bid < 0 and position < 0 and unRealizedPnL > 0 ):
                        c_value = (avgCost - unRealizedPnL) / (abs(position) * 100)
                        ask = round(abs(c_value),4)
                        bid = round(abs(c_value),4)                        
                    
                if (secType == 'STK') and (date != 0):
                    idb.pointWrite('MarketData', [symbol,'ask'], 'ask', ask, barTime)
                    idb.pointWrite('MarketData', [symbol,'bid'], 'bid', bid, barTime)
                    idb.pointWrite('MarketData', [symbol,'open'], 'open', open, barTime)
                    idb.pointWrite('MarketData', [symbol,'close'], 'close', close, barTime)

                DBApp.resAskBidAcctRecordCommit(self, reqId, ask, bid, DBLogic.logic_midPrice_Calculation(ask, bid), unRealizedPnL)
            
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
            query_positionSub = {"subPnL" : False, "subPnLRequest" : False, "secType" : "STK", "$or": [{"position":{"$gte": 100}},{"position":{"$lte": -100}}]}
        if(ActiveFunction == "bpm"):
            query_positionSub = {"subPnL" : False, "subPnLRequest" : False, "secType" : "STK", "$nor": [{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "position":{"$ne": 0}}
        if(ActiveFunction == "option"):
            query_positionSub = {"subPnL" : False, "subPnLRequest" : False, "secType" : "OPT"}

        for r in activeCol.find(query_positionSub):
            ActiveSubProcess = True
            log.info("Add Subscription for PnL Contract: " + str(r.get('conId')))
            conId = r.get('conId')
            query_positionSub1 = {'conId':conId}
            activeCol.update_one(query_positionSub1, {"$set": {"status":True}})
            IBApp.reqAcctPnL(self, conId)
            
        if(ActiveFunction == "pnl"):
            query_positionDeSub = {"$or":[{"subPnL" : True},{"subPnLRequest" : True}], "position": 0, "status" : True, "secType" : "STK"}
        if(ActiveFunction == "bpm"):
            query_positionDeSub = {"$or":[{"subPnL" : True},{"subPnLRequest" : True}], "position": 0, "status" : True, "secType" : "STK"}
        if(ActiveFunction == "option"):
            query_positionDeSub = {"$or":[{"subPnL" : True},{"subPnLRequest" : True}], "position": 0, "status" : True, "secType" : "OPT"}

        for r in activeCol.find(query_positionDeSub):
            ActiveSubProcess = True
            log.info("Remove Subscription for PnL Contract: " + str(r.get('conId')))
            conId = r.get('conId')
            query_positionDeSub1 = {'conId':conId}
            activeCol.update_one(query_positionDeSub1, {"$set": {"status":False}})
            IBApp.reqAcctPnLdisable(self, conId)
        
    #    if (ActiveSubProcess == False):
    #        loop(15)
    #        if (ActiveFunction == "pnl"):
    #            query_subPnL = {"status": True, "secType": "STK", "$or": [{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "$or": [{"positionPrice" : { "$eq": 0 }},{"subPnL" : False}]}
    #            query_subPnLRequest = {"status": True, "$or": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "subPnL" : False, "subPnLRequest" : True}
    #        if (ActiveFunction == "bpm"):
    #            query_subPnL = {"status": True, "secType": "STK", "$nor": [{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "$or": [{"positionPrice" : { "$eq": 0 }},{"subPnL" : False}]}
    #            query_subPnLRequest = {"status": True, "$nor": [{"secType": "OPT"},{"position":{"$gte": 100}},{"position":{"$lte": -100}}], "subPnL" : False, "subPnLRequest" : True}
    #        if (ActiveFunction == "option"):
    #            query_subPnL = {"status": True, "secType": "OPT", "$or": [{"positionPrice" : { "$eq": 0 }},{"subPnL" : False}]}
    #            query_subPnLRequest = {"status": True, "secType": "OPT", "subPnL" : False, "subPnLRequest" : True}

    #        if (activeCol.count_documents(query_subPnL) == activeCol.count_documents(query_subPnLRequest)):
    #            log.info("Cycle PnL Subscription")
    #            activeCol.update_many(query_subPnLRequest,{"$set":{"subPnLRequest" : False}})
                
    def recyclePnLSub(self):
        db = Mongodb()
        if(ActiveFunction == "pnl"):
            query = {"subPnL" : True, "status" : True, "secType" : "STK", "$or": [{"position":{"$gte": 100}},{"position":{"$lte": -100}}]}
        if(ActiveFunction == "bpm"):
            query = {"subPnL" : True, "status" : True, "secType" : "STK", "$nor": [{"position":{"$gte": 100}},{"position":{"$lte": -100}}]}
        if(ActiveFunction == "option"):
            query = {"subPnL" : True, "status" : True, "secType" : "OPT"}
            
        for r in db.recordQueries('Account', query):
            IBApp.sub_stop(self, r.get('realTimeNum'))
            loop(2)
            IBApp.getAcctPnL(self, r.get('realTimeNum'), r.get('conId'))


class DBAppOption(IBApp):
    def reqOptionsInfo(self, symbol, currency, realTimeNum):
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
        priceDate_obj = datetime.datetime.now()
        priceDate_str = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        i = 0
        
        query = {"status" : True, "secType" : "STK", "$or":[{'hedge': True},{'longOptionActive': {"$ne": "none"}}], "optionDownload" : True}        
        i = minExp + (db.recordCount('Account', query) * 180)

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
        
        
        RTNum = randrange(49000)
        
        query = { "realTimeNum" : RTNum }
        query1 = { "realTimeNum" : RTNum }
        query2 = { 'eventType':'Order', 'OrderNumId' : RTNum }
        
        while (activeCol.count_documents(query) > 0) or (activeCol1.count_documents(query1) > 0) or (activeCol2.count_documents(query2) > 0):
            RTNum = randrange(49000)
        return RTNum
    
    def find_nearest(array, value):
        if (len(array) == 0) or (value == 0):
            return 0
        else:
            array = np.asarray(array)
            idx = (np.abs(array - value)).argmin()
            return array[idx]
        
    def logicSelectHedgeOptionTargets(self, reqId):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        array = []
        stockTrigger = 80
        
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
            
            if (position <= -stockTrigger):
                right = "P"
            if (position >= stockTrigger):
                right = "C"
                
            query1 = {"symbol" :  symbol, "right" : right, "priceDate" : {"$lt": priceDate_int}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}} 
            count = activeCol1.count_documents(query1)
            log.info("Evaluating " + symbol + " for Order Generation. Option Records to be updated " + str(count))
            
            if (count != 0):
                try:
                    Mongodb.reqAskBidOptionRequestReset(self, reqId, right, priceDate_int, positionPrice_High, positionPrice_Low)
                    loop_reqAskBidOptionSelect = threading.Thread(target=Mongodb.reqAskBidOptionSelect(self, reqId, right, positionPrice_High, positionPrice_Low))
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
                DBLogic.logicSelectOptionOrders(self, symbol, nearest, position, right, 'hedge')
                
    def logicSelectLongOptionTargets(self, reqId):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        priceDate_obj = datetime.datetime.now()
        priceDate_str  = datetime.datetime.strftime(priceDate_obj, '%s')
        priceDate_int = float(priceDate_str)
        array = []
        global acctRealizedPnL
        
        if(reqId == None):
            query = {'status': True, 'secType': 'STK', 'longOptionActive': {"$ne": "none"}, 'optionDownload': True}
        else:
            query = {'status' : True, 'secType' : 'STK', 'realTimeNum' : reqId, 'longOptionActive': {"$ne": "none"}, 'optionDownload' : True}
        
        for sr in activeCol.find(query):
            reqId = sr.get('realTimeNum')
            symbol = sr.get('symbol')
            position = sr.get('position')
            positionPrice = sr.get('positionPrice')
            avgCost = sr.get('avgCost')
            ask = sr.get('ask')
            bid = sr.get('bid')
            unRealizedPnL = sr.get('unRealizedPnL')
            longOptionActive = sr.get('longOptionActive')
            
            positionPrice_High = positionPrice + (positionPrice * 0.05)
            positionPrice_Low = positionPrice - (positionPrice * 0.05)
            
            if (longOptionActive == "longP"):
                right = "P"
            if (longOptionActive == "longC"):
                right = "C"
                
            query1 = {"symbol":  symbol, "right": right, "priceDate": {"$lt": priceDate_int}, "strike": {"$lt": positionPrice_High, "$gt": positionPrice_Low}} 
            count = activeCol1.count_documents(query1)
            log.info("Evaluating " + symbol + " for Long Order Generation. Option Records to be updated " + str(count))
            
            if (count != 0):
                try:
                    Mongodb.reqAskBidOptionRequestReset(self, reqId, right, priceDate_int, positionPrice_High, positionPrice_Low)
                    loop_reqAskBidOptionSelect = threading.Thread(target=Mongodb.reqAskBidOptionSelect(self, reqId, right, positionPrice_High, positionPrice_Low))
                    loop_reqAskBidOptionSelect.daemon = True
                    loop_reqAskBidOptionSelect.start()
                except Exception as e:
                    log.info("Request AskBid Option ERROR Captured " + str(e))
                               
            if (count == 0):
                log.info("Phase 1 Long Order - Processing Order for " + symbol)
                
                if (acctRealizedPnL <= 20.00):
                    log.info("Phase 1 Long Order - Not enough Realized Gains for Long Option Order " + symbol)
                    return
                    
                query2 = {"symbol" : symbol, "right" : right, "ask" : {"$gt":0.05, "$lt": ((acctRealizedPnL * .50)/100)}, "bid" : {"$gt":0.05, "$lt": ((acctRealizedPnL * .50)/100)}, "strike" : {"$lt": positionPrice_High, "$gt": positionPrice_Low}}
                for rr in activeCol1.find(query2):
                    array.append(rr.get('strike'))
    
                nearest = DBLogic.find_nearest(array, positionPrice)
                log.info("symbol: " + symbol + " Nearest: " + str(nearest) + " Position: " + str(position))
                DBLogic.logicSelectOptionOrders(self, symbol, nearest, position, right, 'long')
        
    def logicSelectOptionOrders(self, symbol, strike, position, right, operation):
        log.info("Phase 2 Order")
        db = Mongodb()
        activeCol1 = self.db['Option']
        startTime = SecStartTime
        endTime = SecEndTime
        orderType = 'LMT'
        algoStrategy = 'Adaptive'
        cashQty = 0.00
        global acctRealizedPnL
        function = 'open'
        array = []
        limitPrice = 0
        stockTrigger = 80
        
        if (operation == 'hedge'):
            query = {"symbol" : symbol, "strike" : strike, "right" : right, "ask" : {"$gt" : 0.25 }, "bid" : {"$gt" : 0.25 }}
            direction = 'SELL'
        elif (operation == 'long'):
            query = {"symbol" : symbol, "strike" : strike, "right" : right, "ask" : {"$gt":0.05, "$lt": ((acctRealizedPnL * .50)/100)}, "bid" : {"$gt":0.05, "$lt": ((acctRealizedPnL * .50)/100)}}
            direction = 'BUY'
        
        for r in activeCol1.find(query):
            array.append(DBLogic.logic_priceComp_Calculation(r.get('ask'), r.get('bid'), direction, 'OPT'))
            limitPrice = np.max(array)
            
        if (limitPrice != 0):
            query1 = {"symbol" : symbol, "strike" : strike, "right" : right, "bid" : {"$eq" : limitPrice}}
            for rr in activeCol1.find(query1):
                if (operation == 'hedge'):
                    limitPosition = math.floor(abs(position)/stockTrigger)
                    direction = 'SELL'
                elif(operation == 'long'):
                    limitPosition = 1
                    direction = 'BUY'
                conId = rr.get('conId')
                secType = rr.get('secType')
                currency = rr.get('currency')
                ask = rr.get('ask')
                bid = rr.get('bid')
                limitPrice = rr.get('bid')
                limitPrice_mid = DBLogic.logic_midPrice_Calculation(ask, bid)
                aRealTimeNum = rr.get('aRealTimeNum')
                exchange = rr.get('exchange')
                log.info("Add Record to OrderDB symbol: " + symbol + " secType: " + secType + " right: " + right + " strikePrice: " + str(strike) + " limitPrice: " + str(limitPrice) + " limitPosition: " + str(limitPosition) + " conId: " + str(conId) + " Direction: " + direction + " startTime: " + str(startTime) + " endTime: " + str(endTime)+  "  RTN: " + str(aRealTimeNum))
                DBOrder.reqOptionOrderDB(self, symbol, secType, right, currency, strike, DBLogic.logic_priceComp_Calculation(ask, bid, direction, secType), cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)

    def logic_evaluateOption_positionSize(self, reqId):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['Option']
        startTime = SecStartTime
        endTime = SecEndTime
        orderType = 'LMT'
        algoStrategy = 'Adaptive'
        cashQty = 0.00
        function = 'updateposition'
        stockTrigger = 80
        
        if(reqId == None):
            query = {"status" : True, "secType" : "STK", "hedge" : False}
        else:
            query = {"status" : True, "secType" : "STK", "hedge" : False, "realTimeNum" : reqId}
        for r in activeCol.find(query):
            symbol = r.get('symbol')
            position = r.get('position')
            sPositionBlock = math.floor(abs(position)/stockTrigger)
            realTimeNum = r.get('realTimeNum')
            
            query1 = {"status" : True, "symbol" : symbol, "secType" : "OPT", "position" : {"$lt": 0}, "ask" : {"$gt": 0.15 }, "bid" : {"$gt": 0.15 }}
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
                        log.info("Add Record to OrderDB symbol: " + symbol + " secType: " + oSecType + " right: " + right + " strikePrice: " + str(oStrike) + " limitPrice: " + str(DBLogic.logic_priceComp_Calculation(oAsk, oBid, direction, oSecType)) + " limitPosition: " + str(limitPosition) + " conId: " + str(oConId) + " Direction: " + direction + " startTime: " + str(startTime) + " endTime: " + str(endTime)+  "  RTN: " + str(aRealTimeNum))
                        DBOrder.reqOptionOrderDB(self, symbol, oSecType, right, oCurrency, oStrike, DBLogic.logic_priceComp_Calculation(oAsk, oBid, direction, oSecType), cashQty, limitPosition, oConId, direction, oExchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function)
                    
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
                    return DBLogic.logic_minPrice_Calculation(mid)
                else:
                    return DBLogic.logic_minPrice_Calculation(ask)
            if (direction == 'BUY'):
                if (ratio >= 0.10):
                    return DBLogic.logic_minPrice_Calculation(mid)
                else:
                    return DBLogic.logic_minPrice_Calculation(bid)
            if (direction == 'Exercise'):
                return DBLogic.logic_minPrice_Calculation(mid)
    
    def logic_duplicateResolution(self, collection, indexField, reqId):
        try:
            loop_duplicate = threading.Thread(target=DBLogic.logic_duplicateResolutionThread(self, collection, indexField, reqId))
            loop_duplicate.start()
        except Exception as e:
            log.info("option general load ERROR Captured " + str(e))
    
    def logic_duplicateResolutionThread(self, collection, indexField, reqId):
        db = Mongodb()
        query = {indexField : reqId}
        for r in db.recordQueries(collection, query):
            query_duplicate = {indexField : r.get(indexField)}
            if (db.recordCount(collection, query_duplicate) > 1):
                log.info("Adjusting Record: " + str(r.get(indexField)))
                new_indexField_value = DBLogic.random_RTP(self)
                data_duplicate = {indexField : new_indexField_value}
                if (collection == "Account"):
                    update_duplicate = {indexField : data_duplicate[indexField], "subPnL" : False, "subPnLRequest" : False}
                    db.recordUpdate(collection, query_duplicate, query_duplicate)  
                    log.info ("de-duplication process completed with Change")
                    return new_indexField_value
                if (collection == "Option"):
                    update_duplicate = {indexField : data_duplicate[indexField]}
                    db.recordUpdate(collection, query_duplicate, update_duplicate)
                    log.info ("de-duplication process completed with Change")
                    return new_indexField_value
            else:
                log.info ("de-duplication process completed with No Change")
                return reqId
            
    def logic_selectPositionsEval(self, conId):
        activeCol = self.db['Account']
        if (conId != 0):
            query_Positions = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, 'conId': conId}
            query_Position_Option = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, 'secType': "OPT", 'conId': conId}
            query_Positions_Refresh = {'status': True, 'subPnL': True, 'positionPrice': {"$eq": 0}, 'conId': conId}
        else:
            query_Positions = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}}
            query_Position_Option = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, 'secType': "OPT"}
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
        
        for rr in activeCol.find(query_Position_Option):
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
        db = Mongodb()
        query_Positions = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, "$or":[{'hedge': True}, {'longOptionActive' : {"$ne":"none"}}], 'optionDownload': False, 'optionDownloadActive': False}
        query_Reset = {'status': True, 'subPnL': True, 'positionPrice': {"$ne": 0}, 'unRealizedPnL':{"$ne": 0}, "$or":[{'hedge': True}, {'longOptionActive' : {"$ne":"none"}}], 'optionDownload': False}
        query_Reset_Update = {'optionDownloadActive': False}
        
        if (db.recordCount('Account', query_Positions) != 0):
            for r in db.recordQueries('Account', query_Positions):
                log.info ("Starting Download of Options: " + r.get('symbol') + " secType: " + r.get('secType') + " reqId: " + str(r.get('realTimeNum')))
                try:
                    loop_reqOptionConEval = threading.Thread(target=Mongodb.reqContractDownloadAcctRecord, args=(self, r.get('realTimeNum')))
                    loop_reqOptionConEval.daemon = True
                    loop_reqOptionConEval.start()
                    loop(360)
                except Exception as e:
                    log.info("reqOptionConEval ERROR Capture " + str(e))
                if(runActive == False):
                    break
        else:
            db.recordUpdates('Account', query_Reset, query_Reset_Update)
            
    def logic_coverOptionStockCompare(self, symbol, option_conId):
        query_stk = {'status': True, 'secType': 'STK', 'symbol': symbol}
        for sr in db.recordQueries('Account', query_stk):
            stk_unRealizedPnL = sr.get('unRealizedPnL')
            stk_avgCost = sr.get('avgCost')
            stk_positionPrice = sr.get('positionPrice')
            
        query_opt = {'status': True, 'secType': 'OPT', 'symbol': symbol, 'condId': option_conId}
        for optrec in db.recordQueries('Account, query_opt'):
            opt_unRealizedPnL = optrec.get('unRealizedPnL')
            opt_strike = optrec.get('strike')
            opt_right = optrec.get('right')
            
        if (opt_right == 'C') & (opt_strike > stk_avgCost) & (stk_positionPrice > opt_strike):
            return False
        else:
            return True
            
        if (opt_right == 'P') & (opt_strike < stk_avgCost) & (stk_positionPrice < opt_strike):
            return False
        else:
            return True
            
    def logic_dailyPercentageChange(secType, position, avgCost, dailyPnL):
        if (secType == 'STK'):
            positionValue = round(abs(position * avgCost),4)
            percentChange = (dailyPnL / positionValue) * 100
            return percentChange
        
        if (secType == 'OPT'):
            positionValue = round(abs(position * avgCost), 4)
            percentChange = (dailyPnL / positionValue) * 100
            return percentChange

    def logic_acctRealizedLossLimit(unRealizedPnL):
        global acctRealizedPnL
        global acctRealizedLossLimit
        
        value = acctRealizedPnL * 0.75
        
        if (value > acctRealizedLossLimit):
            acctRealizedLossLimit = value
            
        if (unRealizedPnL == 0):
            return False
        
        if (unRealizedPnL < 0):
            value1 = acctRealizedPnL - abs(unRealizedPnL)
        if (unRealizedPnL > 0):
            value1 = acctRealizedPnL + abs(unRealizedPnL)
        
        if (value1 > value):
            return True
        else:
            return False


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
    def reqOptionOrderDB(self, symbol, secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function):
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
                    'OrderNumId' : OrderNumId, 'OrderStatus' : "open", 'recDate' : rDate_int, 'aRealTimeNum' : aRealTimeNum, 'function' : function}
            update_data = {'recDate' : data['recDate']}
            
            query_OrderStatus = {"eventType" : "Order", 'symbol' : symbol, 'direction':direction, 'position' : limitPosition, "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}, {"OrderStatus" : {"$eq" : "Transmitted"}},{"OrderStatus" : {"$eq" : "Filled"}}]}
            if (activeCol.count_documents(query_OrderStatus) == 0):
                log.info("Cleared for processing of Order Record for: " + symbol)
                
                if (activeCol.count_documents(query) == 0):
                    activeCol.insert_one(data)
                    log.info("Phase 3 - Inserting new Order Record for: "  + symbol)
                else:  
                    query1 = {'eventType' :  'Order', 'conId' : conId, 'OrderStatus' : 'open', 'limitPrice' : {"$ne": limitPrice}}
                    update_data1 = {'recDate' : data['recDate'], 'limitPrice' : data['limitPrice']}
                    activeCol.update_one(query1, {"$set": update_data1})
                    log.info("Phase 3 - " + str(limitPrice) + " Updated Order Record for: "  + symbol)
            else:
                log.info("Phase 3 - Order DB Record Exist for: " + symbol + " Bypassed")
        except Exception as e:
            log.info("Order DB Record Create ERROR Captured " + str(e))
            
    def reqOptionExerciseOrderDB(self, symbol, secType, right, currency, strike, limitPrice, cashQty, limitPosition, conId, direction, exchange, orderType, algoStrategy, startTime, endTime, aRealTimeNum, function):
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        log.info("Phase 3 - Order DB Exercise Direct for symbol: " + symbol + " Started")
        
        rDate_obj = date.today()
        rDate_str = datetime.datetime.strftime(rDate_obj, '%s')
        rDate_int = float(rDate_str)
        OrderNumId = 0000
        
        try:
            query = {"eventType" : 'OrderExercise', "conId" : conId, "OrderStatus" : {"$ne": 'Cancelled'}}
            data = {'eventType' : 'OrderExercise', 'symbol' : symbol, 'secType' : secType, 'right' : right, 'currency' : currency, 'strike' : strike, 'limitPrice' : limitPrice, 'cashQty' : cashQty, 'limitPosition' : limitPosition, 
                    'conId' : conId, 'direction' : direction, 'exchange' : exchange, 'orderType' : orderType, 'algoStrategy' : algoStrategy, 'startTime' : startTime, 'endTime' : endTime, 
                    'OrderNumId' : OrderNumId, 'OrderStatus' : "open", 'recDate' : rDate_int, 'aRealTimeNum' : aRealTimeNum, 'function' : function}
            update_data = {'recDate' : data['recDate']}
            
            query_OrderStatus = {"eventType" : "OrderExercise", 'symbol' : symbol, 'direction':direction, 'position' : limitPosition, "$or" : [{"OrderStatus" : {"$eq" : "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}}, {"OrderStatus" : {"$eq" : "Transmitted"}},{"OrderStatus" : {"$eq" : "Filled"}}]}
            if (activeCol.count_documents(query_OrderStatus) == 0):
                log.info("Cleared for processing of Order Exercise Record for: " + symbol)
                
                if (activeCol.count_documents(query) == 0):
                    activeCol.insert_one(data)
                    log.info("Phase 3 - Inserting new Order Exercise Record for: "  + symbol)
                else:   
                    query1 = {'eventType' :  'OrderExercise', 'conId' : conId, 'OrderStatus' : 'open', 'limitPrice' : {"$ne": limitPrice}}
                    update_data1 = {'recDate' : data['recDate'], 'limitPrice' : data['limitPrice']}
                    activeCol.update_one(query1, {"$set": update_data1})
                    log.info("Phase 3 - " + str(limitPrice) + " Updated Order Record for: "  + symbol)
            else:
                log.info("Phase 3 - Order DB Record Exercise Exist for: " + symbol + " Bypassed")
        except Exception as e:
            log.info("Order DB Record Create Exercise ERROR Captured " + str(e))
            
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

    def reqOptionOrderEval(self):
        db = Mongodb()
        activeCol3 = self.db['ProcessQueue']
        
        log.info ("Order Record Cleanup")
        
        query = {'OrderStatus' : 'open'} 
        for r in activeCol3.find(query).sort('limitPrice',-1):
            conId = r.get('conId')
            symbol = r.get('symbol')
            
            query_count_conId = {'conId':conId, 'OrderStatus':"open"}
            if (activeCol3.count_documents(query_count_conId) != 1):
                activeCol3.delete_one(query_count_conId)
            
            #query_count_symbol = {'symbol':symbol, 'OrderStatus':"open"}
            #if (activeCol3.count_documents(query_count_symbol) != 1):
            #    activeCol3.delete_one(query_count_symbol)

    def reqOptionOrderCreate(self):
        db = Mongodb()
        activeCol1 = self.db['Account']
        activeCol3 = self.db['ProcessQueue']
        
        global OID
        global buyPower
        global targetBuyPower
        global acacctRealizedPnL
        
        OrderTime_obj = datetime.datetime.now()
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        
        log.info("Phase 4 Order DB Started")
        
        if (buyPower >= targetBuyPower):
            query = {'eventType' : 'Order', 'OrderStatus' : 'open', 'secType' : 'OPT', 'startTime':{"$lt": nowTime}, 'endTime':{"$gt": nowTime}}
            secType = 'OPT'
            log.info ("Option Order Active")
        elif (buyPower < targetBuyPower):
            query = {'eventType' : 'Order', 'OrderStatus' : 'open', 'secType' : 'OPT', 'function' : 'close', 'startTime':{"$lt": nowTime}, 'endTime':{"$gt": nowTime}}
            secType = 'OPT'
            log.info ("Long Option Order Active")
        elif (buyPower < targetBuyPower) and (activeMode == 'BPM'):
            query = {'eventType' : 'Order', 'OrderStatus' : 'open', 'secType' : 'STK', 'startTime':{"$lt": nowTime}, 'endTime':{"$gt": nowTime}}
            secType = 'STK'
            log.info ("BPM Order Active")
        elif (buyPower < targetBuyPower):
            log.info ("BuyPower less than " + str(targetBuyPower))
            return
        
        dcount = activeCol3.count_documents(query)
        log.info ("Order records ready to be processed: " + str(dcount))
        
        if (dcount != 0):
            for r in activeCol3.find(query):
                log.info("Phase 4 Order DB Submit: " + r.get('symbol') + " ContractId: " + str(r.get('conId')))
                
                query_conId = {'status': True, 'conId': r.get('conId')}
                query_conId_PQ = {'OrderStatus': 'open', 'function':'open', 'conId': r.get('conId')}
                if ((db.recordCount('Account', query_conId) != 0) and (r.get('function') == 'open')):
                    data = {'OrderStatus':'Inactive', 'OrderTime':OrderTime_int}
                    update_data = {'OrderStatus' :  data['OrderStatus'], 'OrderTime' : data['OrderTime']}       
                    db.recordUpdate('ProcessQueue', query_conId_PQ, update_data)
                    break
                
                if (activeCol3.count_documents({'eventType':{"$ne":'Order'}, "$or": [{'symbol':r.get('symbol')}, {'conId':r.get('conId')}]}) != 0):
                    log.info ("Ask / Bid Process Request Pending for " + r.get('symbol') + " Waiting")
                    if (secType == 'STK') or (secType == 'OPT'):
                        processQueue.reqHistoricalDataFromQueueTarget(self, historyDelay, r.get('conId'))
                    if (secType == 'OPT'):
                        processQueue.reqHistoricalDataFromQueueTargetOption(self, historyDelay, r.get('symbol'))
                    DBLogic.logic_selectPositionsEval(self, r.get('conId'))
                
                if (r.get('function')=='close'):
                    for rr in activeCol1.find({'conId':r.get('conId')}):
                        if (DBLogic.logic_acctRealizedLossLimit(rr.get('unRealizedPnL')) == False):
                            break

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

    def reqOptionExerciseCreate(self):
        db = Mongodb()
        activeCol3 = self.db['ProcessQueue']
        
        global OID
        global buyPower
        global targetBuyPower
        
        OrderTime_obj = datetime.datetime.now()
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        nowTime = int(datetime.datetime.now().strftime("%H%M%S"))
        
        log.info("Phase 4 Order Exercise DB Started")
        
        if(buyPower >= targetBuyPower * 3):
            query = {'eventType' : 'OrderExercise', 'OrderStatus' : 'open', 'secType' : 'OPT', 'startTime':{"$lt": nowTime}, 'endTime':{"$gt": nowTime}}
            secType = 'OPT'
            log.info ("Order Active")
        elif(buyPower < targetBuyPower * 3):
            log.info ("BuyPower less than " + str(targetBuyPower * 2))
            return
            
        dcount = activeCol3.count_documents(query)
        log.info ("Order records ready to be processed: " + str(dcount))
        
        if (dcount != 0):
            for r in activeCol3.find(query):
                log.info("Phase 4 Exercise DB Submit: " + r.get('symbol') + " ContractId: " + str(r.get('conId')))
                               
                contract = Contract()
                contract.symbol = r.get('symbol')
                contract.secType = r.get('secType')
                contract.exchange = r.get('exchange')
                contract.currency = r.get('currency')
                contract.strike = r.get('strike')
                contract.right = r.get('right')
                contract.conId = r.get('conId')
                
                #order = Order()
                #order.action = r.get('direction')
                #order.orderType = r.get('orderType')
                #if (r.get('orderType') == 'LMT'):
                #    order.totalQuantity = r.get('limitPosition')
                #    order.lmtPrice = round(r.get('limitPrice'),2)
                #    order.tif = 'DAY'
                #if (r.get('orderType') == 'MKT'):
                #    order.cashQty = round(r.get('cashQty'),2)
                #    order.tif = 'IOC'
                #order.transmit = True
                #order.outsideRth = False
                #order.algoStrategy = r.get('algoStrategy')
                
                position = r.get('limitPosition')
                orderId = r.get('OrderNumId')
                
                log.info("OrderNumId Processing for: " + r.get('symbol'))
                
                if (orderId != OID):
                    activeCol3.update_one({'eventType': r.get('eventType'), 'conId':r.get('conId'), 'OrderStatus':'open'}, {"$set": {'OrderNumId':OID}})
                    orderId = OID
                    #order.orderId = OID
                    OID = OID + 1
                    
                log.info("Excerise ORDER Entry: " + str(orderId) + " conId: " + str(r.get('conId')))
                activeCol3.update_one({'OrderNumId' : orderId, 'OrderStatus':'open', 'limitPrice':r.get('limitPrice')}, {"$set": {'OrderStatus' : 'Sent', 'OrderTime' : OrderTime_int}})
                loop(2)
                #IBApp.orderEntry(self, orderId, contract, order)
                IBApp.orderExercise(self, orderId, contract, position)
                loop(2)

    def req_optionOrder_cancelCount(self, symbol, multiplier):
        activeCol = self.db['ProcessQueue']
        query = {'eventType':'Order', 'OrderStatus' : 'Cancelled', 'symbol' : symbol}
        i = 0
        i = activeCol.count_documents(query)
        return (i * multiplier)
    
    def reqOrderActiveCancel(self):
        db = Mongodb()
        activeCol2 = self.db['ProcessQueue']
        
        TDate_obj = date.today()
        TDate_str = datetime.datetime.strftime(TDate_obj, '%s')
        TDate_int = float(TDate_str)
        
        OrderTime_obj = datetime.datetime.now() - datetime.timedelta(seconds=900)
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        OrderTime1_obj = datetime.datetime.now() - datetime.timedelta(seconds=1200)
        OrderTime1_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime1_int = float(OrderTime_str)

        OrderTime2_obj = datetime.datetime.now() - datetime.timedelta(seconds=1500)
        OrderTime2_str = datetime.datetime.strftime(OrderTime1_obj, '%s')
        OrderTime2_int = float(OrderTime1_str)
        
        queryOrderRetry = {'OrderTime':{"$lt": OrderTime_int}, "$or": [{"OrderStatus": {"$eq": "Transmitted"}},{"OrderStatus": {"$eq" : "Sent"}}]}
        query = {'eventType' : "Order" , 'OrderTime':{"$lt": OrderTime1_int}, "$or": [{"OrderStatus": {"$eq": "Submitted"}},{"OrderStatus" : {"$eq" : "PreSubmitted"}},{"OrderStatus" : {"$eq" : "ApiPending"}},{"OrderStatus" : {"$eq" : "PendingSubmit"}}]}
        query1 = {'eventType' : "Order" , 'OrderTime':{"$lt": OrderTime2_int}, "$or": [{"OrderStatus": {"$eq": "Submitted"}}, {"OrderStatus" : {"$eq" : "PreSubmitted"}},{"OrderStatus" : {"$eq" : "ApiPending"}},{"OrderStatus" : {"$eq" : "PendingSubmit"}}]}
        query2 = {"$or" : [{"recDate" : {"$lt" : TDate_int}}, {"OrderStatus" : {"$eq": "Inactive"}}]}
        
        for rr in activeCol2.find(queryOrderRetry):
            log.info("Phase 6 Order")
            orderId = rr.get('OrderNumId')
            conId = rr.get('conId')
            IBApp.orderCancel(self, orderId)
            log.info ("Switched order over to open status from transmitted: " + str(orderId) + " for conId: " + str(conId))
            activeCol2.update_one({'OrderNumId' : orderId, 'conId': conId}, {"$set": {'OrderNumId':0,'OrderStatus':'open'}})
        
        for r in activeCol2.find(query):
            log.info("Phase 6 Order")
            orderId = r.get('OrderNumId')
            IBApp.orderCancel(self, orderId)
        loop(2)
        # Removed Stuck Orders
        activeCol2.delete_many(query1)
        activeCol2.delete_many(query2)
    
    def resOptionOrderStatus(self, conId, orderId, status):
        db = Mongodb()
                
        log.info("Phase 5 Order Update")
        OrderTime_obj = datetime.datetime.now()
        OrderTime_str = datetime.datetime.strftime(OrderTime_obj, '%s')
        OrderTime_int = float(OrderTime_str)
        
        query = {'OrderNumId':orderId, 'OrderStatus':{"$ne":status}}
        data = {'OrderStatus':status, 'OrderTime':OrderTime_int}
        update_data = {'OrderStatus' :  data['OrderStatus'], 'OrderTime' : data['OrderTime']}       
        db.recordUpdate('ProcessQueue', query, update_data)
        
        if (status == 'Filled'):
            loop(5)
            AcctOrderStatusUpdate = threading.Thread(target=DBOrder.resAcctOrderStatusUpdate, args=(self, conId, orderId, status))
            AcctOrderStatusUpdate.daemon = True
            AcctOrderStatusUpdate.start()
        
    def resAcctOrderStatusUpdate (self, conId, orderId, status):
        db = Mongodb()
        recordUpdateReady = False
        query_acct = {'OrderNumId': orderId, 'OrderStatus': 'Filled', 'secType': 'OPT'}
        for rec in db.recordQueries('ProcessQueue', query_acct):
            function = rec.get('function')
            account = localHostAccount
            symbol = rec.get('symbol')
            conId = rec.get('conId')
            secType = rec.get('secType')
            currency = rec.get('currency')
            direction = rec.get('direction')
            expDate = ''
            avgCost = 0.00
            right = rec.get('right')
            strike = rec.get('strike')
            exchange = rec.get('exchange')
            position = rec.get('position')
            
            if (function == 'close'):
                position = 0
                DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, expDate, position, avgCost, right, strike, exchange)
                recordUpdateReady = True
            
            if (function == 'open') and (direction == 'SELL'):
                position = -(rec.get('limitPosition'))
                DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, expDate, position, avgCost, right, strike, exchange)
                recordUpdateReady = True
                
            if (function == 'open') and (direction == 'BUY'):
                position = (rec.get('limitPosition'))
                avgCost = (position * 100) * (rec.get('limitPrice'))
                DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, expDate, position, avgCost, right, strike, exchange)
                recordUpdateReady = True
                
            if (function == 'updateposition'):
                aquery = {'conId' : conId}
                for aRec in db.recordQueries('Account', aquery):
                    position = aRec.get('position') + rec.get('limitPosition')
                    avgCost = ((rec.get('limitPosition')*100) * rec.get('limitPrice')) + (aRec.get('avgCost'))
                DBApp.resAddAcctRecord(self, account, symbol, conId, secType, currency, expDate, position, avgCost, right, strike, exchange)
                recordUpdateReady = True
                
            if (recordUpdateReady == True):
                query_update = {'OrderNumId': orderId}
                update_data = {'function': 'complete'}
                db.recordUpdate('ProcessQueue', query_update, update_data)

    
class processQueue(IBApp):
    def clearProcessQueue(self):
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}]}
        db.delAcctRecords('ProcessQueue', query)
        log.info("ProcessQueue Collection Cleared")
        
    def clearProcessQueueFlag(self):
        db = Mongodb()
        query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}]}
        data = {'sent':False}
        db.recordUpdates('ProcessQueue', query, data)
        log.info("ProcessQueue Sent Flag Cleared")
        
    def clearProcessQueueOrder(self, OrderId):
        db = Mongodb()
        query = {"OrderNumId":OrderId , "$or": [{'eventType':'Order'},{'eventType':'OrderExercise'}]}
        data = {'OrderStatus':'open', 'OrderNumId':0}
        db.recordUpdate('ProcessQueue', query, data)
        log.info("ProcessQueue Order Cleared on Error")
    
    def reqHistoricalDataQueue(self, reqId, symbol, conId, secType, source, duration):
        db = Mongodb()
        dateTimeNow_obj = datetime.datetime.now() 
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        query = {'reqId' : reqId}
        if (source == 'Account'):        
            data = {'eventType':'Historical Account', 'reqId':reqId, 'symbol':symbol, 'conId':conId, 'secType':secType, 'recDate': dateTimeNow_int, 'lastDate': dateTimeNow_int, 'sent':False, 'duration': duration}
        if (source == 'Option'):
            data = {'eventType':'Historical Option', 'reqId':reqId, 'symbol':symbol, 'conId':conId, 'secType':secType, 'recDate': dateTimeNow_int, 'lastDate': dateTimeNow_int, 'sent':False, 'duration': duration}
        update_data = {'lastDate':data['lastDate']}
        db.addUniqueRecord('ProcessQueue', query, data, update_data)
        
    def reqHistoricalDataFromQueue(self, delay):
        if(ActiveFunction  == "optionHis"):
            loop (int(ConCount))
            try:
                loop_dataFromQueue = threading.Thread(target=processQueue.reqHistoricalDataFromQueueThread, args=(self, delay))
                loop_dataFromQueue.start()
            except Exception as e:
                log.info("Historical Data from Queue ERROR Captured " + str(e))
        
        if(ActiveFunction  == "accountHis"):
            loop(int(ConCount))
            try:
                loop_dataFromQueue1 = threading.Thread(target=processQueue.reqHistoricalDataFromQueueThread1, args=(self, delay))
                loop_dataFromQueue1.daemon = True
                loop_dataFromQueue1.start()
            except Exception as e:
                log.info("Historical Data from Queue1 ERROR Captured " + str(e))
            try:
                loop_dataFromQueue2 = threading.Thread(target=processQueue.reqHistoricalDataFromQueueThread2, args=(self, delay))
                loop_dataFromQueue2.daemon = True
                loop_dataFromQueue2.start()
            except Exception as e:
                log.info("Historical Data from Queue2 ERROR Captured " + str(e))
            try:
                loop_dataFromQueue3 = threading.Thread(target=processQueue.reqHistoricalDataFromQueueThread3, args=(self, delay))
                loop_dataFromQueue3.daemon = True
                loop_dataFromQueue3.start()
            except Exception as e:
                log.info("Historical Data from Queue2 ERROR Captured " + str(e))
            try:
                loop_dataFromQueue4 = threading.Thread(target=processQueue.reqHistoricalDataFromQueueThread4, args=(self, delay))
                loop_dataFromQueue4.daemon = True
                loop_dataFromQueue4.start()
            except Exception as e:
                log.info("Historical Data from Queue2 ERROR Captured " + str(e))

    def reqHistoricalDataFromQueueThread(self, delay):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        global QT0
        i = 0
        
        if (QT0 == False):
            QT0 = True
            query_core = {'status' : True, 'secType' : 'STK', "$or":[{'hedge': True},{'longOptionActive': {"$ne": "none"}}], 'optionDownload' : True, 'AskBidActive' : False}
            for sr in db.recordQueries('Account', query_core):
                query_core1 = {'status' : True, 'secType' : 'STK', "$or":[{'hedge': True},{'longOptionActive': {"$ne": "none"}}], 'optionDownload' : True, 'realTimeNum' : sr.get('realTimeNum')}
                data_core1 = {'AskBidActive' : True}
                update_data_core1 = {'AskBidActive': data_core1['AskBidActive']}
                db.recordUpdate('Account', query_core1, update_data_core1)
                
                query_opt = {'eventType': "Historical Option", 'symbol': sr.get('symbol'), 'sent': False}
                for r in db.recordQueries('ProcessQueue', query_opt):
                    IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                    query_opt1 = {"reqId" : r.get('reqId')}
                    data_opt1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                    update_data_opt1 = {'lastDate':data_opt1['lastDate'], 'sent':data_opt1['sent']}
                    db.recordUpdate('ProcessQueue', query_opt1, update_data_opt1)
                    loop(delay)
                    i = i + 1
                    if(runActive == False) or (r.get('duration') != '60 S'):
                        break
            
                
                data_core1 = {'AskBidActive' : False}
                update_data_core1 = {'AskBidActive': data_core1['AskBidActive']}
                db.recordUpdate('Account', query_core1, update_data_core1)
    
                # Hedge Queue Status Reset
                query_reset = {'status' : True, 'secType' : 'STK', 'hedge' :  True, 'optionDownload' : True, 'AskBidActive' : True}
                data_reset = {'AskBidActive' : False}
                update_reset = {'AskBidActive':data_reset['AskBidActive']}
                db.recordUpdates('Account', query_reset, update_reset)
                
                # Option processQueue Reset
                query_reset1 = {'eventType':'Historical Option', 'sent' : True}
                data_reset1 = {'sent' : False}
                update_reset1 = {'sent':data_reset1['sent']}
                db.recordUpdates('Account', query_reset1, update_reset1)
            
            QT0 = False

    def reqHistoricalDataFromQueueTargetOption(self, delay, symbol):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        query_core = {'status' : True, 'secType' : 'STK', "$or":[{'hedge': True},{'longOptionActive': {"$ne": "none"}}], 'optionDownload' : True, 'AskBidActive' : False, 'symbol': symbol}
        for sr in db.recordQueries('Account', query_core):
            query_core1 = {'status' : True, 'secType' : 'STK', "$or":[{'hedge': True},{'longOptionActive': {"$ne": "none"}}], 'optionDownload' : True, 'realTimeNum' : sr.get('realTimeNum')}
            data_core1 = {'AskBidActive' : True}
            update_data_core1 = {'AskBidActive': data_core1['AskBidActive']}
            db.recordUpdate('Account', query_core1, update_data_core1)
            
            query_opt = {'eventType': "Historical Option", 'symbol': sr.get('symbol'), 'sent': False}
            for r in db.recordQueries('ProcessQueue', query_opt):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                query_opt1 = {"reqId" : r.get('reqId')}
                data_opt1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_opt1 = {'lastDate':data_opt1['lastDate'], 'sent':data_opt1['sent']}
                db.recordUpdate('ProcessQueue', query_opt1, update_data_opt1)
                loop(delay)
                if(runActive == False) or (r.get('duration') != '60 S'):
                    break

    def reqHistoricalDataFromQueueThread1(self, delay):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        global QT1
        i = 0
        
        if (QT1 == False):
            QT1 = True
            query_acct = {'eventType' : "Historical Account", 'sent' : False, 'duration': {"$eq": "60 S"}}
            #for r in activeCol1.find(query_acct).sort('recDate',1):
            for r in db.recordQueries('ProcessQueue', query_acct):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                query_acct1 = {"reqId" : r.get('reqId')}
                data_acct1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_acct1 = {'lastDate':data_acct1['lastDate'], 'sent':data_acct1['sent']}
                #activeCol1.update_one(query_acct1, update_data_acct1)
                db.recordUpdate('ProcessQueue', query_acct1, update_data_acct1)
                loop(delay)
                i = i + 1
                if(i >= 90):
                    break
                if(runActive == False) or (r.get('duration') != '60 S'):
                    break
            
            # Hedge Queue Status Reset
            query_reset = {'status' : True, 'secType' : 'STK', 'hedge' :  True, 'optionDownload' : True, 'AskBidActive' : True}
            data_reset = {'AskBidActive' : False}
            update_reset = {'AskBidActive':data_reset['AskBidActive']}
            #activeCol.update_many(query_reset, update_reset)
            db.recordUpdates('Account', query_reset, update_reset)
            
            #Account processQueue Reset
            query_reset1 = {'eventType':'Historical Account', 'sent' : True}
            data_reset1 = {'sent' : False}
            update_reset1 = {'sent':data_reset1['sent']}
            #activeCol1.update_many(query_reset1, update_reset1)
            db.recordUpdates('ProcessQueue', query_reset1, update_reset1)
            
            QT1 = False

    def reqHistoricalDataFromQueueThread2(self, delay):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        query_immediate = {'status': True, 'sent' : False, 'positionPrice': {"$eq": 0 }, 'duration': {"$eq": "60 S"}}
        #for i in activeCol.find(query_immediate).sort('recDate',1):
        for i in db.recordQueries('Account', query_immediate):
            query_acct_immediate = {'eventType' : "Historical Account", 'sent' : False, 'reqId': i.get('realTimeNum')}
            #for r in activeCol1.find(query_acct_immediate).sort('recDate',1):
            for r in db.recordQueries('ProcessQueue', query_acct_immediate):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                query_acct1 = {"reqId" : r.get('reqId')}
                data_acct1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_acct1 = {'lastDate':data_acct1['lastDate'], 'sent':data_acct1['sent']}
                #activeCol1.update_one(query_acct1, update_data_acct1)
                db.recordUpdate('ProcessQueue', query_acct1, update_data_acct1)
                loop(delay)
                if(runActive == False) or (r.get('duration') != '60 S'):
                    break

    def reqHistoricalDataFromQueueThread3(self, delay):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        query_immediate = {'status': True, 'sent' : False, 'unRealizedPnL': {"$gt": -10.00 }, 'duration': {"$eq": "60 S"}}
        #for i in activeCol.find(query_immediate).sort('unRealizedPnL',-1):
        for i in db.recordQueries('Account', query_immediate):
            query_acct_immediate = {'eventType' : "Historical Account", 'sent' : False, 'reqId': i.get('realTimeNum')}
            #for r in activeCol1.find(query_acct_immediate).sort('recDate',1):
            for r in db.recordQueries('ProcessQueue', query_acct_immediate):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                query_acct1 = {"reqId" : r.get('reqId')}
                data_acct1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_acct1 = {'lastDate':data_acct1['lastDate'], 'sent':data_acct1['sent']}
                #activeCol1.update_one(query_acct1, update_data_acct1)
                db.recordUpdate('ProcessQueue', query_acct1, update_data_acct1)
                loop(delay)
                if(runActive == False) or (r.get('duration') != '60 S'):
                    break

    def reqHistoricalDataFromQueueThread4(self, delay):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        query_immediate = {'status': True, 'sent' : False, 'duration': {"$ne": "60 S"}}
        #for i in activeCol.find(query_immediate).sort('unRealizedPnL',-1):
        for i in db.recordQueries('Account', query_immediate):
            query_acct_immediate = {'eventType' : "Historical Account", 'sent' : False, 'reqId': i.get('realTimeNum')}
            #for r in activeCol1.find(query_acct_immediate).sort('recDate',1):
            for r in db.recordQueries('ProcessQueue', query_acct_immediate):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                query_acct1 = {"reqId" : r.get('reqId')}
                data_acct1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_acct1 = {'lastDate':data_acct1['lastDate'], 'sent':data_acct1['sent']}
                #activeCol1.update_one(query_acct1, update_data_acct1)
                db.recordUpdate('ProcessQueue', query_acct1, update_data_acct1)
                loop(delay)
                if(runActive == False) or (r.get('duration') != '60 S'):
                    break

    def reqHistoricalDataFromQueueTarget(self, delay, conId):
        db = Mongodb()
        activeCol = self.db['Account']
        activeCol1 = self.db['ProcessQueue']

        dateTimeNow_obj = datetime.datetime.now()
        dateTimeNow_str = datetime.datetime.strftime(dateTimeNow_obj, '%s')
        dateTimeNow_int = float(dateTimeNow_str)
        
        query_immediate = {'status': True, 'sent' : False, 'conId': conId, 'duration': {"$eq": "60 S"}}
        #for i in activeCol.find(query_immediate).sort('unRealizedPnL',-1):
        for i in db.recordQueries('Account', query_immediate):
            query_acct_immediate = {'eventType' : "Historical Account", 'sent' : False, 'reqId': i.get('realTimeNum')}
            #for r in activeCol1.find(query_acct_immediate).sort('recDate',1):
            for r in db.recordQueries('ProcessQueue', query_acct_immediate):
                IBApp.getAskBid(self, r.get('reqId'), r.get('symbol'), r.get('conId'), r.get('secType'), r.get('duration'))
                query_acct1 = {"reqId" : r.get('reqId')}
                data_acct1 = {'lastDate' : dateTimeNow_int, 'sent': True}
                update_data_acct1 = {'lastDate':data_acct1['lastDate'], 'sent':data_acct1['sent']}
                #activeCol1.update_one(query_acct1, update_data_acct1)
                db.recordUpdate('ProcessQueue', query_acct1, update_data_acct1)
                loop(delay)
                if(runActive == False) or (r.get('duration') != '60 S'):
                    break
            
    def reqHistoricalDataResetQueue(self):
        db = Mongodb()
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
        db = Mongodb()
        activeCol = self.db['ProcessQueue']
        if (reqId == None):
            query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'conId':conId, 'sent':True}
        else:
            query = {"$or": [{'eventType':'Historical Account'},{'eventType':'Historical Option'}], 'reqId':reqId, 'sent':True}
        activeCol.delete_many(query)
    
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
    
    def reqIDLock_AskBidOption(self, symbol, action):
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
        
    def reqIDLock_positionMulti(self, reqId, action):
        global reqID_lock_positionMulti
        
        if (reqID_lock_positionMulti.count(reqId) > 0) and (action == 'lock'):
            return False
        else:
            reqID_lock_positionMulti.append(reqId)
            return True
        
        if (reqID_lock_positionMulti.count(reqId) > 0 ) and (action == 'unlock'):
            reqID_lock_positionMulti.remove(reqId)
            return True
        else:
            return False
        

def main(argv):
    global runActive
    global Container
    global ConCount
    global ActiveFunction
    
    if os.environ.get('CONTAINER') != None:
        pass
    else:
        os.environ['CONTAINER'] = 'NO'
    
    if (os.environ.get('CONTAINER') == 'YES'):
        log.info("Using Environment Variables")
        Container = True
        ConCount = os.environ['CONCOUNT']
        ActiveFunction = os.environ['ACTIVEFUNCTION']
    else:
        log.info("Using Command Line Variables")
        try:
            opts, args = getopt.getopt(argv, "c:f:", ["ConCount=", "ActiveFunction="])
        except getopt.GetoptError:
            log.info("Missing Option -c value  or --ConCount=value")
            log.info("Missing Option -f value  or --ActvieFunction=[pnl / batch / bpm / option /position / contract]")
    
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


def forceReset(self):
    db = Mongodb()
    global runActive
    db.client.close()
    IBApp.stop(self)
    runActive = False


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
