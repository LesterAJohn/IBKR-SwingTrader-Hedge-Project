#region Using declarations
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using System.Windows.Media;
using System.Xml;
using System.Xml.Serialization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using System.Globalization;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Gui.SuperDom;
using NinjaTrader.Gui.Tools;
using NinjaTrader.Data;
using NinjaTrader.NinjaScript;
using NinjaTrader.Core.FloatingPoint;
using NinjaTrader.NinjaScript.Indicators;
using NinjaTrader.NinjaScript.DrawingTools;
#endregion

//This namespace holds Strategies in this folder and is required. Do not change it. 
namespace NinjaTrader.NinjaScript.Strategies
{

	public class XmlApplicationConfig
	{
		private const String filename = "C:\\NinjaTraderConfig\\AutoTradeConfig.xml";
		private static Boolean liqValue;
		private static Double otpValue;
		private static Double bspValue;
		private static String convertToCurrency;

		public Boolean liquidate()
		{
			if (liqValue == null)
            {
				readConfigurationFile();
            }
			return liqValue;
		}

		public Double optionTrigger()
		{
			if (otpValue == null)
            {
				readConfigurationFile();
            }
			return otpValue;
		}

		public Double buysellPressure()
		{
			if (bspValue == null)
			{
				readConfigurationFile();
			}
			return bspValue;
		}

		public String useCurrency()
        {
			if (convertToCurrency == null)
            {
				readConfigurationFile();
            }
			return convertToCurrency;
        }

		public void readConfigurationFile()
        {
			XmlReader reader = null;

			if (CheckFile(filename) == false)
			{
				Console.WriteLine("File not Found");
			}
			else
			{
				try
				{

					// Load the reader with the data file and ignore all white space nodes.
					XmlReaderSettings settings = new XmlReaderSettings();
					settings.IgnoreWhitespace = true;
					reader = XmlReader.Create(filename, settings);
					{
						// Position on ATSetup Configurations
						reader.ReadToFollowing("ATSetup");

						XmlReader inner = reader.ReadSubtree();

						inner.ReadToDescendant("liquidate");
						liqValue = inner.ReadElementContentAsBoolean();
						Console.WriteLine(liqValue);

						inner.MoveToNextAttribute();
						//inner.ReadToDescendant("optionTriggerPer");
						otpValue = (inner.ReadElementContentAsDouble() / 100);
						Console.WriteLine(otpValue);

						inner.MoveToNextAttribute();
						//inner.ReadToDescendant("buysellPressure");
						bspValue = inner.ReadElementContentAsDouble();
						Console.WriteLine(bspValue);

						inner.MoveToNextAttribute();
						//inner.ReadToDescendant("activeCurrency");
						convertToCurrency = inner.ReadElementContentAsString();
						Console.WriteLine(bspValue);

						inner.Close();
					}
				}

				finally
				{
					if (reader != null)
						reader.Close();
				}
			}
		}

		private bool CheckFile(string FileName)
		{
			try
			{
				return File.Exists(FileName);
			}
			catch (IOException e)
			{
				return false;
			}
		}
	}

	public class SL : Strategy
	{
		private double AskBidSpread;
		private double FQuant;
		private int Status;
		private bool TrailStatus;
		private double FBuyingPower;
		private double AcctPercentage;
		private double TargetPercentage;
		private double TargetPercentageInterval;
		private double TargetPercentageIntervalDown;
		private double STS;
		private double SPS;
		private double MP;
		private double BAS;
		private bool Exit;
		private double PA;
		private double PQ;
		private double PT;
		private double PASL;
		private double PAPTT;
		private double PTG;
		private double PTGPercentage;
		private double RSISlope;
		private double RSISlopeD;
		private double ADXSlope;
		private double ADXSlopeD;
		private double RecADXSlope;
		private double BBSlope;
		private double RecProfitTrigger;
		private int EEBars;
		private int StartTimePre;
		private int StartTime;
		private int ReviewTime;
		private int CloseTime;
		private int MarketCloseTime;
		private int MarketCloseTimePost;
		private bool FeeActive;
		private bool LossOrder;
		private double LossBorderPercentage;
		private double EODMarginTarget;
		private double OpeningAcctValue;
		private double BuyPowerMargin;
		private double BPR_TargetPercentage;
		private double BPR_TargetPercentage_Saved;
		private bool TradedToday;
		private bool Under25KActive;
		private bool NetLiqEmulation;
		private bool ExcessIntradayMarginEmulation;

		// XML Configurations
		private bool liquid;
		private double optionTrigger;
		private double buysellPressure;
		private string useCurrency;
		
		// Specialize Function Variables
		private ATR ATR1;
		private MACD MACD1;
		private MACD MACD2;

		private Bollinger Bollinger5M;

		private double BSPA;
		private double BSPB;

		private EMA EMAH0;
		private EMA EMAH1;
		private EMA EMAH2;
		private EMA EMAH3;
		private EMA EMAC0;
		private EMA EMAC1;
		private EMA EMAC2;
		private EMA EMAC3;
		private EMA EMAL0;
		private EMA EMAL1;
		private EMA EMAL2;
		private EMA EMAL3;
		
		private EMA	EMAH50;
		private EMA	EMAH51;
		private EMA	EMAH52;
		private EMA	EMAH53;
		private EMA	EMAC50;
		private EMA	EMAC51;
		private EMA	EMAC52;
		private EMA	EMAC53;
		private EMA	EMAL50;
		private EMA	EMAL51;
		private EMA	EMAL52;
		private EMA	EMAL53;

		private Order entryOrder;
		private Order exitOrder;

		// static HttpClient client = new HttpClient();

		XmlApplicationConfig XmlConfig = new XmlApplicationConfig();

		// Calculate Functions
		
		private void PositionConfiguration()
		{
			PA = PositionAccount.AveragePrice;
			PQ = PositionAccount.Quantity;
			PT = PATR();
				
			if(PositionAccount.MarketPosition == MarketPosition.Long)
			{
				PASL = PA - PT*2;
				PAPTT = PA + TPATR();
				RecADXSlope = Slope(ADX(High,10),10,0);
				RecProfitTrigger = PA + PT;
			}
				
			if(PositionAccount.MarketPosition == MarketPosition.Short)
			{
                PASL = PA + PT*2;
				PAPTT = PA - TPATR();
				RecADXSlope = Slope(ADX(Low,10),10,0);
				RecProfitTrigger = PA - PT;
			}
		}

		private double PositionSize()
		{
			if (State == State.Realtime)
			{
				if (BuyPower() != 0)
				{
					FBuyingPower = BuyPower() * AcctPercentage;
					FQuant = Math.Truncate((FBuyingPower / MidPriceBar()));
				}
				else
				{
					FQuant = 0;
				}
			}
			return FQuant;
		}

		private double PositionSizeforOption()
		{
			double AvailBP;
			double PosGap;

			if (State == State.Realtime)
			{
				if (BuyPower() != 0 && Math.Abs(Position.Quantity) < 100)
				{
					AvailBP = BuyPower() * AcctPercentage;
					PosGap = (100 - Math.Abs(Position.Quantity));
					if ((AvailBP / PosGap) > MidPriceBar())
					{
						FQuant = Math.Truncate((AvailBP / MidPriceBar()));
					}
					else
					{
						FQuant = PositionSize();
					}
				}
				else
				{
					FQuant = 0;
				}
			}
			return FQuant;
		}

		private int PositionSizeOnLoss()
		{
			if (Position.MarketPosition != MarketPosition.Flat && Position.Quantity >= 1)
			{
				int Value = 0;
				if (RealizedPL() > 0 && BuyPower() != 0)
				{
					Value = Convert.ToInt32(Math.Truncate(Convert.ToDecimal(Math.Abs((RealizedPL() * .50) / MidPriceBar()))));
				}
				else if (BuyPower() == 0 && RealizedPL() > -(NetLiq() * 0.001) && RealizedPL() > 0 && TimeFunction() < CloseTime)
				{
					Value = Convert.ToInt32(Math.Truncate(Convert.ToDecimal(Math.Abs((RealizedPL() * .75) / MidPriceBar()))));
				}
				else if (BuyPower() == 0 && RealizedPL() > -(NetLiq() * 0.001) && RealizedPL() <= 0 && TimeFunction() < CloseTime)
				{
					Value = Convert.ToInt32(Math.Truncate(Convert.ToDecimal(Math.Abs((RealizedPL() + (NetLiq() * 0.001)) / MidPriceBar()))));
				}
				else if (BuyPower() == 0 && TimeFunction() > CloseTime && TimeFunction() < MarketCloseTime - 0001)
				{
					Value = 1;
				}

				if (Value >= 1)
				{
					return Value;
				}
				else
				{
					return 0;
				}
			}
			return 0;
		}

		private double BuyPower()
		{
			double CV;
			double BPV;
			double BP;

			CV = CashValue();
			BPV = BuyPowerValue();

			if (BPV > NetLiq() * 0.001)
			{
				BP = BPV;
			}
			else
			{
				BP = 0;
				return 0;
			}

			if (BP != 0 && NetLiq() > conversion("USD", 210000)) return (BP * 0.70);
			if (BP != 0 && NetLiq() > conversion("USD", 175000)) return (BP * 0.75);
			if (BP != 0 && NetLiq() > conversion("USD", 140000)) return (BP * 0.80);
			if (BP != 0 && NetLiq() > conversion("USD", 105000)) return (BP * 0.85);
			if (BP != 0 && NetLiq() > conversion("USD", 70000)) return (BP * 0.90);
			if (BP != 0 && NetLiq() > conversion("USD", 35000)) return (BP * 0.95);

			return BP;
		}
		
		private double BuyPowerMarginPercentage()
		{
			double BPV;

			BPV = BuyPowerValue();

			if (NetLiq() - OpeningAcctValue < 0.00)
			{
				if (NetLiq() - OpeningAcctValue < 0.00 && NetLiq() - OpeningAcctValue > conversion("USD", -1000.00)) return (0.25);
				if (NetLiq() - OpeningAcctValue < conversion("USD", -1000.00)) return (0.50);
			}
			else
			{
				if (BPV != 0 && BPV >= conversion("USD", 50000)) return (0.05);
				if (BPV != 0 && BPV >= conversion("USD", 40000)) return (0.10);
				if (BPV != 0 && BPV >= conversion("USD", 30000)) return (0.20);
				if (BPV != 0 && BPV >= conversion("USD", 20000)) return (0.30);
				if (BPV != 0 && BPV >= conversion("USD", 10000)) return (0.40);
			}
			
			return (0.50);
		}

		private double CashValue()
		{
			return conversion("USD", Account.Get(AccountItem.CashValue, Currency.UsDollar));
			// return Account.Get(AccountItem.CashValue, Currency.UsDollar);
		}

		private double BuyPowerValue()
		{
			return conversion("USD", Account.Get(AccountItem.BuyingPower, Currency.UsDollar));
			// return Account.Get(AccountItem.BuyingPower, Currency.UsDollar);
		}

		private bool BPR()
		{
			if (BuyPower() == 0 && TargetPercentage != BPR_TargetPercentage)
			{
				BPR_TargetPercentage_Saved = TargetPercentage;
				TargetPercentage = BPR_TargetPercentage;
				return true;
			}

			if (BuyPower() == 0)
			{
				return true;
			}

			if (BuyPower() != 0 && BPR_TargetPercentage_Saved != 0.00)
			{
				TargetPercentage = BPR_TargetPercentage_Saved;
				BPR_TargetPercentage_Saved = 0.00;
				return false;
			}
			return false;
		}

		private void BuyPowerMarginUD()
		{
			if (BuyPower() != 0)
			{
				double Value1 = BuyPower() * (BuyPowerMarginPercentage());

				if (Value1 > BuyPowerMargin) BuyPowerMargin = Value1;
			}
		}

		private double NetLiq()
		{
			if (NetLiqEmulation == false)
			{
				return conversion("USD", Account.Get(AccountItem.NetLiquidation, Currency.UsDollar));
				// return Account.Get(AccountItem.NetLiquidation, Currency.UsDollar);
			}
			else
			{
				if (BuyPowerValue() > CashValue())
				{
					return BuyPowerValue();
				}
				else
				{
					return CashValue();
				}
			}
		}

		private double RealizedPL()
		{
			return conversion("USD", Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar));
			// return Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar);
		}

		private double UnRealizedPL()
		{
			return conversion("USD", Account.Get(AccountItem.UnrealizedProfitLoss, Currency.UsDollar));
			// return Account.Get(AccountItem.UnrealizedProfitLoss, Currency.UsDollar);
		}

		private double AURPnLPercentage()
		{
			return ((UnRealizedPL() / NetLiq())*100);
		}
		
		private double IntraDayMargin()
		{
			if(ExcessIntradayMarginEmulation == false)
			{
				return conversion("USD", Account.Get(AccountItem.ExcessIntradayMargin, Currency.UsDollar));
				// return Account.Get(AccountItem.ExcessIntradayMargin, Currency.UsDollar);
			}
			else
			{
				if (BuyPowerValue() > CashValue())
				{
					return BuyPowerValue();
				}
				else
				{
					return CashValue();
				}
			}
		}

		private bool EODMarginMgt()
		{
			if (IntraDayMargin() >= EODMarginTarget)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		private double PnLPosition()
		{
			double Value = 0.00;

			// Investigate
			// if (Position.MarketPosition == MarketPosition.Long) Value = converstion("USD", Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentBid()));
			// if (Position.MarketPosition == MarketPosition.Short) Value = conversion("USD", Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentAsk()));

			// if (Position.MarketPosition == MarketPosition.Long) Value = Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentBid());
			// if (Position.MarketPosition == MarketPosition.Short) Value = Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentAsk());

			if (Position.MarketPosition == MarketPosition.Long)
			{ 
				Value = (GetCurrentBid() - PA) * Math.Abs(PQ);
			}

			if (Position.MarketPosition == MarketPosition.Short)
            {
				Value = (PA - GetCurrentAsk()) * Math.Abs(PQ);
            }

			return Value;
		}

		private double PnLPercentage()
		{
			// Investigate
			return ((PnLPosition() / Math.Abs(PA * PQ)) * 100);
		}

		private double SingleSharePnL()
		{
			return (PnLPosition() / PQ);
		}

		private bool DayTradeActive()
		{
			if (NetLiq() > conversion("USD",25000))
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		private bool NetProfitL()
		{
			bool ProfitStatus;
			if (conversion("USD", Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar)) > NetLiq() * TargetPercentage)
			// if (Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar) > NetLiq() * TargetPercentage)
				{
					ProfitStatus = true;
				}
			else
				{
					ProfitStatus = false;
				}
			return ProfitStatus;
		}
		
		private void TargetAdjustment(string Type)
		{			
			if (TimeFunction() >= ReviewTime && TimeFunction() < MarketCloseTime && Type == "Time" || TimeFunction() < MarketCloseTime && Type == "TrailStop")
				{
					AcctPercentage = 0.10;

					if (NetProfitL() == true)
						{
							TargetPercentage = TargetPercentage + TargetPercentageInterval;
							BPR_TargetPercentage = TargetPercentage / 2;
						}

					if (TargetPercentage < 0.001)
						{
							TargetPercentage = 0.001;
						}

					while (PositionSize() != 0 && PositionSize() < 10 && AcctPercentage < 0.90)
						{
							AcctPercentage = AcctPercentage + 0.01;
						}

					if (BuyPower() != 0)
						{
							PTGPercentage = 1.00;
						}
					else
						{
							PTGPercentage = 0.25;
						}

					BuyPowerMarginUD();
					ReviewTime = TimeFunction() + 0005;
					// Debug_AccountValue();
				}
		}

		private bool PositionAcceptLoss()
		{
			double Value = 0.00;

			if (Position.MarketPosition == MarketPosition.Long) Value = conversion("USD", Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar)) + Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentBid());
			if (Position.MarketPosition == MarketPosition.Short) Value = conversion("USD", Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar)) + Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentAsk());

			// if (Position.MarketPosition == MarketPosition.Long) Value = Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar) + Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentBid());
			// if (Position.MarketPosition == MarketPosition.Short) Value = Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar) + Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentAsk());

			if (Value > (NetLiq() * TargetPercentage))
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		private bool PositionAcceptLossOnLoss()
		{
			double Value = 0.00;

			Value = conversion("USD", Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar)) + ((SingleSharePnL()*PositionSizeOnLoss()));
			// Value = Account.Get(AccountItem.RealizedProfitLoss, Currency.UsDollar) + ((SingleSharePnL()*PositionSizeOnLoss()));
			
			if (Value > (NetLiq() * TargetPercentage))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
		
		private bool FeeCovered()
		{
			if (Position.MarketPosition == MarketPosition.Long && GetCurrentBid() > PA + Fee())
			{
				return true;
			}
			
			if (Position.MarketPosition == MarketPosition.Short && GetCurrentAsk() < PA - Fee())
			{
				return true;
			}			
			return false;
		}
		
		private bool FeeCoveredForTrail()
		{
			if (BreakEven() != 0)
				{
					SetPositionStatus();
		
					if (TrailStatus == true)
						{
							return true;
						}
				}
			return false;
		}
		
		private void SetPositionStatus()
		{
			// Set - Trail Reset
			if (Position.MarketPosition == MarketPosition.Flat && Position.Quantity == 0 && entryOrder == null && BarsSinceExitExecution(0, "", 0) == -1
				|| Position.MarketPosition == MarketPosition.Flat && Position.Quantity == 0 && entryOrder == null && BarsSinceExitExecution(0, "", 0) > 3)
				{
					Status = 1;
				}
			
			// Set - Trade Status
			if (Position.MarketPosition == MarketPosition.Long && PA != 0 && Status != 4 && entryOrder == null
				|| Position.MarketPosition == MarketPosition.Short && PA != 0 && Status != 4 && entryOrder == null)
				{
					Status = 2;
				}
			
			// Trigger Profits
			if(BreakEven() != 0 && Status == 2 && FeeCovered() == true)
				{
					Status = 3;
				}

			// Trail Target Launch
			if(BreakEven() != 0 && TrailStatus == false && FeeCovered() == true)
			{
				if(Position.MarketPosition == MarketPosition.Long && GetCurrentBid() > (PA + TPATR()) && GetCurrentBid() > PAPTT) 
					{
						PTG = BreakEven();
						PAPTT = PAPTTCompute();
						TrailStatus = true;
					}

				if(Position.MarketPosition == MarketPosition.Short && GetCurrentAsk() < (PA - TPATR()) && GetCurrentAsk() < PAPTT)
					{
						PTG = BreakEven();
						PAPTT = PAPTTCompute();
						TrailStatus = true;
					}
			}
		}

		private double BreakEven()
		{
			double BEValue = FixProfit() + Fee();

			// Long Positon
			if (Position.MarketPosition == MarketPosition.Long && PnLPercentage() > (PTGPercentage + 0.2) && PnLPosition() > BEValue)
			{
				SPS = GetCurrentBid() - PA;
				return SPS * 0.1;
			}
			
			// Short Position
			if (Position.MarketPosition == MarketPosition.Short && PnLPercentage() > (PTGPercentage + 0.2) && PnLPosition() > BEValue)
			{
				SPS = PA - GetCurrentAsk();
				return SPS * 0.1;
			}
			SPS = 0;
			return SPS;
		}

		private bool EOSActive()
		{
			if (EODMarginMgt() == false || NetLiq() < OpeningAcctValue )
			{ 
				return true;
			}
			else
			{
				return false;
			}
		}

		private double MidPriceBar()
		{
			// Compute Mid Price of existing bar
			return ((Highs[3][0] + Lows[3][0]) / 2);
		}
		
		private double MidPriceDailyBar()
		{
			// Compute Mid Price of Daily bar
			return ((Highs[1][0] + Lows[1][0]) / 2);
		}
		
		private double BidAskSpread()
		{
			return (((GetCurrentAsk() - GetCurrentBid()) / BidAskMidPrice()) * 100);
		}
		
		private double BidAskMidPrice()
		{
			return((GetCurrentAsk() + GetCurrentBid()) / 2);
		}
	
		private double MaxLoss()
		{
			// Ticksize Cal
			return (Math.Abs(Math.Truncate(1.00 / TickSize)));
		}
		
		private double MaxProfit()
		{
			// Ticksize Call
			return (Math.Abs(Math.Truncate(Position.GetMarketPrice()*.001) / TickSize));
		}
		
		private bool DMCrossExit()
		{
			if (Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentBid()) < 0 
				&& Position.MarketPosition == MarketPosition.Long && (DM(3).DiMinus[0] > DM(5).DiPlus[0]))
				{
					Exit = true;
					return Exit;
				}
				
			if (Position.GetUnrealizedProfitLoss(PerformanceUnit.Currency, GetCurrentAsk()) < 0 
				&& Position.MarketPosition == MarketPosition.Short && (DM(3).DiMinus[0] < DM(5).DiPlus[0]))
				{
					Exit = true;
					return Exit;
				}
				Exit = false;
				return Exit;
		}
		
		private int TimeFunction()
		{
	      	DateTime localDate = DateTime.Now;
			int Time = Int32.Parse(localDate.ToString("HHmm"));
			return Time;
		}

		private double Radius(double Value)
		{
			return (Math.Atan(Value) * (180/Math.PI));
		}

        private double PASLCompute()
        {
            // Calculate Position Average Stop Loss
            if (Position.MarketPosition == MarketPosition.Long && PASL < GetCurrentBid() - PT*2)
                PASL = GetCurrentBid() - PT*2;

            if (Position.MarketPosition == MarketPosition.Short && PASL > GetCurrentAsk() + PT*2)
                PASL = GetCurrentAsk() + PT*2;

            return PASL;
        }

		private double PAPTTCompute()
        {
            // Calculate  Profit Trail Target
            if (Position.MarketPosition == MarketPosition.Long && PAPTT < (GetCurrentBid() - PTG))
                PAPTT = GetCurrentBid() - PTG;
            
			if (Position.MarketPosition == MarketPosition.Short && PAPTT > (GetCurrentAsk() + PTG))
                PAPTT = GetCurrentAsk() + PTG;
            
			return PAPTT;
        }

		private double FixProfit()
		{
			double FixPro;
			FixPro = 0;
			
			if (Position.Quantity != 0)
			{
				if (PA < conversion("USD", 100)) FixPro = 0.10;
				if (PA >= conversion("USD", 100) && PA < conversion("USD", 200)) FixPro = 0.15;
				if (PA >= conversion("USD", 200) && PA < conversion("USD", 300)) FixPro = 0.20;
				if (PA >= conversion("USD", 300)) FixPro = 0.25;
			
				if(FeeActive == true)
					{
					return conversion("USD", FixPro + Fee());
					// return FixPro + Fee();
					}
				else
					{
					return conversion("USD", FixPro);
					// return FixPro;
					}
			}
			return conversion("USD", FixPro);
			// return FixPro;
		}
		
		private double PATR()
		{
			if(FeeActive == true)
			{
				return Math.Abs(ATR(14)[0] + Fee());
			}
			else
			{
				return Math.Abs(ATR(14)[0]);
			}
		}
		
		private double TPATR()
		{
			double Value = 0.00;
			Value = PATR() + (PATR() * 0.2);
			
			if (FeeActive == true)
			{
				return Value + Fee();
			}
			else
			{
				return Value;
			}
		}

		private double Fee()
		{
			double FeeValuePS;
			
			if (Position.MarketPosition != MarketPosition.Flat && Position.Quantity * .005 < 1)
			{
				FeeValuePS = (1 / Position.Quantity);
			}
			else
			{
				FeeValuePS = .005;
			}
			FeeValuePS = FeeValuePS + (0.000119 * Position.Quantity);
			FeeValuePS = FeeValuePS + (0.0000221 * (Position.Quantity * PA));
			FeeValuePS = FeeValuePS + (0.50 / Position.Quantity);

			return conversion("USD", (FeeValuePS*2));
			// return (FeeValuePS*2);
		}
		
		private double MaxHighPrice()
		{ 
			return (High[1]);
		}

		private double MaxLowPrice()
		{
			return (Low[1]);
		}

		private bool ADXSlopeStatus()
		{
			bool ADXStatus;
			ADXStatus = false;

			if (Position.MarketPosition == MarketPosition.Long && Slope(ADX(High,10),10,0) < RecADXSlope)
			{
				ADXStatus = true;
				RecADXSlope = RecADXSlope * 2;
			}
			if (Position.MarketPosition == MarketPosition.Short && Slope(ADX(Low,10),10,0) < RecADXSlope)
			{
				ADXStatus = true;
				RecADXSlope = RecADXSlope * 2;
			}
			return ADXStatus;
		}

		private string LTTrendStatus()
		{
			// Long Trend
			if ((EMAH1[0] > EMAH2[0]) && (EMAH0[0] > EMAH1[0])
				&& (GetCurrentAsk() > Opens[1][0])
				&& (GetCurrentAsk() > Closes[1][1]))
					{
						return "long";
					}

			// Short Trend
			if ((EMAL1[0] < EMAL2[0]) && (EMAL0[0] < EMAL1[0])
				&& (GetCurrentBid() < Opens[1][0])
				&& (GetCurrentBid() < Closes[1][1]))
					{
						return "short";
					}
			return "null";
		}
		
		private string LTTrendStatus5M()
		{
			// Long Trend
			if ((EMAH50[0] > EMAH51[0])
				&& (GetCurrentAsk() > Opens[2][0])
				&& (GetCurrentAsk() > Closes[2][1]))
					{
						return "long";
					}

			// Short Trend
			if ((EMAL50[0] < EMAL51[0])
				&& (GetCurrentBid() < Opens[2][0])
				&& (GetCurrentBid() < Closes[2][1]))
					{
						return "short";
					}
			return "null";
		}

		private void BSPCount(string count)
        {
			if (count == "ask") BSPA = BSPA + 1;
			if (count == "bid") BSPB = BSPB + 1;
			if (count == "reset") { BSPA = 0; BSPB = 0; }
        }

		private double BSPCal(string direction)
        {
			double value = 0;

			if (direction == "buy")
            {
				value = ((BSPA / (BSPA + BSPB)) * 100);
            }

			if (direction == "sell")
            {
				value = ((BSPB / (BSPA + BSPB)) * 100);
			}

			return value;
        }

		private string BuySellP()
		{
			if (BSPCal("buy") > BSPCal("sell") && BSPCal("buy") > buysellPressure)
				return "buy";

			if (BSPCal("sell") > BSPCal("buy") && BSPCal("sell") > buysellPressure)
				return "sell";

			return "null";
		}

		private bool HoldExit()
		{
			// Based on index-0 1-min bars
			if (Position.MarketPosition == MarketPosition.Long && Closes[0][2] < Opens[0][1] && Opens[0][1] <  Closes[0][1] && Closes[0][1] < Opens[0][0] && BuySellP() == "buy" && BuyPower() != 0 && PnLPercentage() < (PTGPercentage * 2)
				|| Position.MarketPosition == MarketPosition.Short && Closes[0][2] > Opens[0][1] && Opens[0][1] >  Closes[0][1] && Closes[0][1] > Opens[0][0] && BuySellP() == "sell" && BuyPower() != 0 && PnLPercentage() < (PTGPercentage * 2))
				{
					return true;
				}
				else
				{
					return false;
				}
		}

		private double conversion(string cvto, double oValue)
        {
			double cValue = 0.00;

			if (cvto == "USD") cValue = oValue * 1.00;
			if (cvto == "GBP") cValue = oValue * 0.73543;

			return cValue;
        }

		// Open-Close Execution Functions

		private void OpenLong(string Title, int OrderT)
		{
			if (BidAskSpread() > AskBidSpread)
			{
				if (GetCurrentBid() < Low[0])
				{
					if (OrderT == 0) EnterLongLimit(0, false, Convert.ToInt32(PositionSize()), GetCurrentBid(), Title);
					if (OrderT == 1) EnterLongLimit(0, false, Convert.ToInt32(PositionSizeOnLoss()), GetCurrentBid(), Title);
					if (OrderT == 2) EnterLongLimit(0, false, Convert.ToInt32(PositionSizeforOption()), GetCurrentBid(), Title);
				}
				else
				{
					if (OrderT == 0) EnterLongLimit(0, false, Convert.ToInt32(PositionSize()), Low[0], Title);
					if (OrderT == 1) EnterLongLimit(0, false, Convert.ToInt32(PositionSizeOnLoss()), Low[0], Title);
					if (OrderT == 2) EnterLongLimit(0, false, Convert.ToInt32(PositionSizeforOption()), Low[0], Title);
				}
			}
			else
			{
				if (OrderT == 0) EnterLongLimit(0, false, Convert.ToInt32(PositionSize()), BidAskMidPrice(), Title);
				if (OrderT == 1) EnterLongLimit(0, false, Convert.ToInt32(PositionSizeOnLoss()), BidAskMidPrice(), Title);
				if (OrderT == 2) EnterLongLimit(0, false, Convert.ToInt32(PositionSizeforOption()), BidAskMidPrice(), Title);
			}
		}

		private void OpenShort(string Title, int OrderT)
		{
			if (BidAskSpread() > AskBidSpread)
			{
				if (GetCurrentAsk() > High[0])
				{
					if (OrderT == 0) EnterShortLimit(0, false, Convert.ToInt32(PositionSize()), GetCurrentAsk(), Title);
					if (OrderT == 1) EnterShortLimit(0, false, Convert.ToInt32(PositionSizeOnLoss()), GetCurrentAsk(), Title);
					if (OrderT == 2) EnterShortLimit(0, false, Convert.ToInt32(PositionSizeforOption()), GetCurrentAsk(), Title);
				}
				else
				{
					if (OrderT == 0) EnterShortLimit(0, false, Convert.ToInt32(PositionSize()), High[0], Title);
					if (OrderT == 1) EnterShortLimit(0, false, Convert.ToInt32(PositionSizeOnLoss()), High[0], Title);
					if (OrderT == 2) EnterShortLimit(0, false, Convert.ToInt32(PositionSizeforOption()), High[0], Title);
				}
			}
			else
			{
				if (OrderT == 0) EnterShortLimit(0, false, Convert.ToInt32(PositionSize()), BidAskMidPrice(), Title);
				if (OrderT == 1) EnterShortLimit(0, false, Convert.ToInt32(PositionSizeOnLoss()), BidAskMidPrice(), Title);
				if (OrderT == 2) EnterShortLimit(0, false, Convert.ToInt32(PositionSizeforOption()), BidAskMidPrice(), Title);
			}
		}

		private void CloseLong(string Title, int OrderT)
		{
			if (BidAskSpread() > AskBidSpread)
			{
				if (GetCurrentAsk() > High[0])
				{
					if (OrderT == 0) ExitLongLimit(Convert.ToInt32(Position.Quantity), GetCurrentAsk(), Title, "");
					if (OrderT == 1) ExitLongLimit(PositionSizeOnLoss(), GetCurrentAsk(), Title, "");
				}
				else
				{
					if (OrderT == 0) ExitLongLimit(Convert.ToInt32(Position.Quantity), High[0], Title, "");
					if (OrderT == 1) ExitLongLimit(PositionSizeOnLoss(), High[0], Title, "");
				}
			}
			else
			{
				if (OrderT == 0) ExitLongLimit(Convert.ToInt32(Position.Quantity), BidAskMidPrice(), Title, "");
				if (OrderT == 1) ExitLongLimit(PositionSizeOnLoss(), BidAskMidPrice(), Title, "");
			}
		}
		
		private void CloseShort(string Title, int OrderT)
		{
			if (BidAskSpread() > AskBidSpread)
			{
				if (GetCurrentBid() < Low[0])
				{
					if (OrderT == 0) ExitShortLimit(Convert.ToInt32(Position.Quantity), GetCurrentBid(), Title, "");
					if (OrderT == 1) ExitShortLimit(PositionSizeOnLoss(), GetCurrentBid(), Title, "");
				}
				else
				{
					if (OrderT == 0) ExitShortLimit(Convert.ToInt32(Position.Quantity), Low[0], Title, "");
					if (OrderT == 1) ExitShortLimit(PositionSizeOnLoss(), Low[0], Title, "");
				}
			}
			else
			{
				if (OrderT == 0) ExitShortLimit(Convert.ToInt32(Position.Quantity), BidAskMidPrice(), Title, "");
				if (OrderT == 1) ExitShortLimit(PositionSizeOnLoss(), BidAskMidPrice(), Title, "");
			}
		}

		// Debug Functions
		
		private void Debug_Order()
		{
			//Log Functions
				Log ("_______________________", LogLevel.Information);
				Log ("Market: " + Position.MarketPosition, LogLevel.Information);
				Log ("Status: " + Status, LogLevel.Information);
				Log ("entryOrderValue: " + entryOrder, LogLevel.Information);
				Log ("ASK: " + GetCurrentAsk(), LogLevel.Information);
				Log ("MH: " + MaxHighPrice(), LogLevel.Information);
				Log ("ML: " + MaxLowPrice(), LogLevel.Information);
				Log ("DM3: " + DM(3).DiMinus[0] + " " + DM(3).DiPlus[0], LogLevel.Information);
				Log ("DM10: " + DM(10).DiMinus[0] + " " + DM(10).DiPlus[0], LogLevel.Information);
				Log ("DM30: " + DM(30).DiMinus[0] + " " + DM(30).DiPlus[0], LogLevel.Information);
				Log ("SlopeDM: " + Slope(DM(10).DiPlus,10,0), LogLevel.Information);
				Log ("SlopeADX: " + Slope(ADX(High,10),10,0), LogLevel.Information);
				Log ("Slope Value: " + ADXSlope, LogLevel.Information);
				Log ("ADXHigh: " + ADX(High,10)[0], LogLevel.Information);
				Log ("SlopeRSI: " + Slope(RSI(High,15,3),15,0), LogLevel.Information);
				Log ("Slope Value: " + RSISlope, LogLevel.Information);
				Log ("RSI: " + RSI(High,15,3)[0], LogLevel.Information);
				Log ("BID: " + GetCurrentBid(), LogLevel.Information);
				Log ("Open: " + Opens[0][0], LogLevel.Information);
				Log ("Close: " + Closes[0][1], LogLevel.Information);
				Log ("PositionSize: " + PositionSize(), LogLevel.Information);
				Log ("EMAH0: " + EMAH0[0] + " " + EMAH1[0] + " " + EMAH2, LogLevel.Information);
				Log ("EMAC0: " + EMAC0[0] + " " + EMAC1[0] + " " + EMAC2, LogLevel.Information);
				Log ("EMAL0: " + EMAL0[0] + " " + EMAL1[0] + " " + EMAL2, LogLevel.Information);
				Log ("_______________________", LogLevel.Information);
		}

		private void Debug_AccountValue()
		{
			//Log Functions
				Log ("_______________________", LogLevel.Information);
				Log ("SecurityName: " + Position.Instrument, LogLevel.Information);
				Log ("ReviewTime: " + ReviewTime, LogLevel.Information);
				Log ("OpenAccountValue: " + OpeningAcctValue, LogLevel.Information);
				Log ("BuyPowerMargin:" + BuyPowerMargin, LogLevel.Information);
				Log ("_______________________", LogLevel.Information);
		}

		// Open Functions
		
		private void OpenPosition()
		{
			// Open Long
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (DM(3).DiMinus[0] < DM(3).DiPlus[0]) && (DM(10).DiMinus[0] < DM(10).DiPlus[0]) && (DM(30).DiMinus[0] < DM(30).DiPlus[0])
				&& (Slope(DM(10).DiPlus,10,0) > 0)
				&& (Slope(ADX(High,10),10,0) > ADXSlope) && (ADX(High,10)[0] > 20)
				&& (Slope(RSI(High,15,3),15,0) > RSISlope) && (RSI(High,15,3)[0] < 80)
				&& (GetCurrentAsk() > Opens[0][0])
				&& (GetCurrentAsk() > Closes[0][1])
				&& (LTTrendStatus5M() == "long")
				&& (PositionSize() > 10)
				)
					{
						OpenLong("BuyOrder", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (DM(3).DiMinus[0] > DM(3).DiPlus[0]) && (DM(10).DiMinus[0] > DM(10).DiPlus[0]) && (DM(30).DiMinus[0] > DM(30).DiPlus[0])
				&& (Slope(DM(10).DiMinus,10,0) > 0)
				&& (Slope(ADX(Low,10),10,0) > ADXSlope) && (ADX(Low,10)[0] > 20)
				&& (Slope(RSI(Low,15,3),15,0) < - RSISlope) && (RSI(Low,15,3)[0] > 20)
				&& (GetCurrentBid() < Opens[0][0])
				&& (GetCurrentBid() < Closes[0][1])
				&& (LTTrendStatus5M() == "short")
				&& (PositionSize() > 10)
				)
					{
						OpenShort("ShortOrder", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionST()
		{
			// Open Long
			if (
				(Position.MarketPosition == MarketPosition.Long && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (DM(3).DiMinus[0] < DM(3).DiPlus[0]) && (DM(10).DiMinus[0] < DM(10).DiPlus[0]) && (DM(30).DiMinus[0] < DM(30).DiPlus[0])
				&& (Slope(DM(10).DiPlus,10,0) > 0)
				&& (Slope(ADX(High,10),10,0) > ADXSlope) && (ADX(High,10)[0] > 20)
				&& (Slope(RSI(High,15,3),15,0) > RSISlope) && (RSI(High,15,3)[0] < 80)
				&& (GetCurrentAsk() > Opens[0][0])
				&& (GetCurrentAsk() > Closes[0][1])
				&& (LTTrendStatus5M() == "long")
				&& (PositionSize() > 10)
				)
					{
						OpenLong("BuyOrderST", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Short && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (DM(3).DiMinus[0] > DM(3).DiPlus[0]) && (DM(10).DiMinus[0] > DM(10).DiPlus[0]) && (DM(30).DiMinus[0] > DM(30).DiPlus[0])
				&& (Slope(DM(10).DiMinus,10,0) > 0)
				&& (Slope(ADX(Low,10),10,0) > ADXSlope) && (ADX(Low,10)[0] > 20)
				&& (Slope(RSI(Low,15,3),15,0) < - RSISlope) && (RSI(Low,15,3)[0] > 20)
				&& (GetCurrentBid() < Opens[0][0])
				&& (GetCurrentBid() < Closes[0][1])
				&& (LTTrendStatus5M() == "short")
				&& (PositionSize() > 10)
				)
					{
						OpenShort("ShortOrderST", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionLTN()
		{
			// Open Long
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (DM(3).DiMinus[0] < DM(3).DiPlus[0]) && (DM(10).DiMinus[0] < DM(10).DiPlus[0]) && (DM(30).DiMinus[0] < DM(30).DiPlus[0])
				&& (Slope(DM(10).DiPlus,10,0) > 0)
				&& (Slope(ADX(High,10),10,0) > ADXSlope) && (ADX(High,10)[0] > 20)
				&& (Slope(RSI(High,15,3),15,0) > RSISlope) && (RSI(High,15,3)[0] < 80)
				&& (LTTrendStatus() == "long")
				&& (PositionSize() > 10)
				)
					{
						OpenLong("BuyOrderLTN", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (DM(3).DiMinus[0] > DM(3).DiPlus[0]) && (DM(10).DiMinus[0] > DM(10).DiPlus[0]) && (DM(30).DiMinus[0] > DM(30).DiPlus[0])
				&& (Slope(DM(10).DiMinus,10,0) > 0)
				&& (Slope(ADX(Low,10),10,0) > ADXSlope) && (ADX(Low,10)[0] > 20)
				&& (Slope(RSI(Low,15,3),15,0) < - RSISlope) && (RSI(Low,15,3)[0] > 20)
				&& (LTTrendStatus() == "short")
				&& (PositionSize() > 10)
				)
					{
						OpenShort("ShortOrderLTN", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionLT()
		{
			// Open Long
			if (
				(Position.MarketPosition == MarketPosition.Long && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (DM(3).DiMinus[0] < DM(3).DiPlus[0]) && (DM(10).DiMinus[0] < DM(10).DiPlus[0]) && (DM(30).DiMinus[0] < DM(30).DiPlus[0])
				&& (Slope(DM(10).DiPlus,10,0) > 0)
				&& (Slope(ADX(High,10),10,0) > ADXSlope) && (ADX(High,10)[0] > 20)
				&& (Slope(RSI(High,15,3),15,0) > RSISlope) && (RSI(High,15,3)[0] < 80)
				&& (LTTrendStatus() == "long")
				&& (PositionSize() > 10)
				)
					{
						OpenLong("BuyOrderLT", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Short && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (DM(3).DiMinus[0] > DM(3).DiPlus[0]) && (DM(10).DiMinus[0] > DM(10).DiPlus[0]) && (DM(30).DiMinus[0] > DM(30).DiPlus[0])
				&& (Slope(DM(10).DiMinus,10,0) > 0)
				&& (Slope(ADX(Low,10),10,0) > ADXSlope) && (ADX(Low,10)[0] > 20)
				&& (Slope(RSI(Low,15,3),15,0) < - RSISlope) && (RSI(Low,15,3)[0] > 20)
				&& (LTTrendStatus() == "short")
				&& (PositionSize() > 10)
				)
					{
						OpenShort("ShortOrderLT", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionBSPN()
		{
			// Open Long
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (BuySellP() == "buy")
				&& (Slope(RSI(High, 15, 3), 15, 0) > RSISlope) && (RSI(High, 15, 3)[0] < 80)
				&& (LTTrendStatus() == "long")
				&& (PositionSize() > 10)
				)
			{
				OpenLong("BuyOrderBSPN", 0);
				Status = 2;
				//Debug_Order();
			}

			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (BuySellP() == "sell")
				&& (Slope(RSI(Low, 15, 3), 15, 0) < -RSISlope) && (RSI(Low, 15, 3)[0] > 20)
				&& (LTTrendStatus() == "short")
				&& (PositionSize() > 10)
				)
			{
				OpenShort("ShortOrderBSPN", 0);
				Status = 2;
				//Debug_Order();
			}
		}

		private void OpenPositionBSP()
		{
			// Open Long 
			if (
				(Position.MarketPosition == MarketPosition.Long && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (BuySellP() == "buy")
				&& (Slope(RSI(High, 15, 3), 15, 0) > RSISlope) && (RSI(High, 15, 3)[0] < 80)
				&& (LTTrendStatus() == "long")
				&& (PositionSize() > 10)
				)
			{
				OpenLong("BuyOrderBSP", 0);
				Status = 2;
				//Debug_Order();
			}

			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Short && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (BuySellP() == "sell")
				&& (Slope(RSI(Low, 15, 3), 15, 0) < -RSISlope) && (RSI(Low, 15, 3)[0] > 20)
				&& (LTTrendStatus() == "short")
				&& (PositionSize() > 10)
				)
			{
				OpenShort("ShortOrderBPS", 0);
				Status = 2;
				//Debug_Order();
			}
		}

		private void OpenPositionOptionReady()
		{
			// Open Long
			if (
				(Position.MarketPosition == MarketPosition.Long && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (LTTrendStatus() == "long")
				&& (Math.Abs(Position.Quantity) < 100)
				&& (PnLPosition() < -(NetLiq() * optionTrigger))
				&& (PositionSizeforOption() > 1)
				)
					{
						OpenLong("BuyOrderLTOption", 2);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if (
				(Position.MarketPosition == MarketPosition.Short && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (LTTrendStatus() == "short")
				&& (Math.Abs(Position.Quantity) < 100)
				&& (PnLPosition() < -(NetLiq() * optionTrigger))
				&& (PositionSizeforOption() > 1)
				)
					{
						OpenShort("ShortOrderLTOption", 2);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionBBN()
		{
			// Open Long 5-min Bars
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (CrossAbove(Highs[2], Bollinger5M.Middle, 1))
				&& (Radius(Slope(Bollinger5M.Middle, 5, 0)) > (BBSlope))
				&& ((Bollinger5M.Upper[0] - Bollinger5M.Lower[0]) > (Bollinger5M.Upper[1] - Bollinger5M.Lower[1]))
				&& ((Bollinger5M.Upper[1] - Bollinger5M.Lower[1]) > (Bollinger5M.Upper[2] - Bollinger5M.Lower[2]))
  				// && (Closes[2][1] < Opens[2][0])
				&& (PositionSize() > 10)
				)
					{
						OpenLong("BuyOrderBBN", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short 5-min Bars
			if (
				(Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (CrossBelow(Lows[2], Bollinger5M.Middle, 1))
				&& (Radius(Slope(Bollinger5M.Middle, 5, 0)) < -(BBSlope))
				&& ((Bollinger5M.Upper[0] - Bollinger5M.Lower[0]) > (Bollinger5M.Upper[1] - Bollinger5M.Lower[1]))
				&& ((Bollinger5M.Upper[1] - Bollinger5M.Lower[1]) > (Bollinger5M.Upper[2] - Bollinger5M.Lower[2]))
				// && (Closes[2][1] > Opens[2][0])
				&& (PositionSize() > 10)
				)
					{
						OpenShort("ShortOrderBBN", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionBB()
		{
			// Open Long 5-min Bars
			if (
				(Position.MarketPosition == MarketPosition.Long && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (CrossAbove(Highs[2], Bollinger5M.Middle, 1))
				&& (Radius(Slope(Bollinger5M.Middle, 5, 0)) > (BBSlope))
				&& ((Bollinger5M.Upper[0] - Bollinger5M.Lower[0]) > (Bollinger5M.Upper[1] - Bollinger5M.Lower[1]))
				&& ((Bollinger5M.Upper[1] - Bollinger5M.Lower[1]) > (Bollinger5M.Upper[2] - Bollinger5M.Lower[2]))
				// && (Closes[2][1] < Opens[2][0])
				&& (PositionSize() > 10)
				)
					{
						OpenLong("BuyOrderBB", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short 5-min Bars
			if (
				(Position.MarketPosition == MarketPosition.Short && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (CrossBelow(Lows[2], Bollinger5M.Middle, 1))
				&& (Radius(Slope(Bollinger5M.Middle, 5, 0)) < -(BBSlope))
				&& ((Bollinger5M.Upper[0] - Bollinger5M.Lower[0]) > (Bollinger5M.Upper[1] - Bollinger5M.Lower[1]))
				&& ((Bollinger5M.Upper[1] - Bollinger5M.Lower[1]) > (Bollinger5M.Upper[2] - Bollinger5M.Lower[2]))
				// && (Closes[2][1] > Opens[2][0])
				&& (PositionSize() > 10)
				)
					{
						OpenShort("ShortOrderBB", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionOnLoss()
		{
			// Open Long
			if ((Position.MarketPosition == MarketPosition.Long && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentAsk() < MaxHighPrice())
				&& (DM(3).DiMinus[0] < DM(3).DiPlus[0]) && (DM(10).DiMinus[0] < DM(10).DiPlus[0]) && (DM(30).DiMinus[0] < DM(30).DiPlus[0])
				&& (Slope(DM(10).DiPlus,10,0) > 0)
				&& (Slope(ADX(High,10),10,0) > ADXSlope) && (ADX(High,10)[0] > 20)
				&& (Slope(RSI(High,15,3),15,0) > RSISlope) && (RSI(High,15,3)[0] < 80)
				&& (LTTrendStatus() == "long")
				&& (PositionSize() >= 1)
				&& (LossOrder == false))
					{
						OpenLong("BuyOrderLoss", 1);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if ((Position.MarketPosition == MarketPosition.Short && Status == 2)
				&& (entryOrder == null)
				&& (GetCurrentBid() > MaxLowPrice())
				&& (DM(3).DiMinus[0] > DM(3).DiPlus[0]) && (DM(10).DiMinus[0] > DM(10).DiPlus[0]) && (DM(30).DiMinus[0] > DM(30).DiPlus[0])
				&& (Slope(DM(10).DiMinus,10,0) > 0)
				&& (Slope(ADX(Low,10),10,0) > ADXSlope) && (ADX(Low,10)[0] > 20)
				&& (Slope(RSI(Low,15,3),15,0) < - RSISlope) && (RSI(Low,15,3)[0] > 20)
				&& (LTTrendStatus() == "short")
				&& (PositionSize() >= 1)
				&& (LossOrder == false))
					{
						OpenShort("ShortOrderLoss", 1);
						Status = 2;
						//Debug_Order();
					}			
		}

		private void OpenPositionUnder25k()
		{
			// Open Long
			if ((Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (LTTrendStatus() == "long")
				&& (PositionSize() >= 1)
				&& (DayTradeActive() == false))
					{
						OpenLong("BuyOrder25K", 0);
						Status = 2;
						//Debug_Order();
					}
			
			// Open Short
			if ((Position.MarketPosition == MarketPosition.Flat && Status == 1)
				&& (entryOrder == null)
				&& (LTTrendStatus() == "short")
				&& (PositionSize() >= 1)
				&& (DayTradeActive() == false)
				&& (NetLiq() > 5000))
					{
						OpenShort("ShortOrder25K", 0);
						Status = 2;
						//Debug_Order();
					}			
		}

		// Close Functions
		
		private void ClosePositionADX()
		{
			if ((Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && PnLPercentage() > PTGPercentage && TrailStatus == false && ADXSlopeStatus() == true)
				|| (Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && PnLPercentage() > PTGPercentage && TrailStatus == false && ADXSlopeStatus() == true))
					{
						if (FeeCoveredForTrail() == true)
							{
								TrailStatus = true;
							}
						else
							{
								if (Position.MarketPosition == MarketPosition.Long) {CloseLong("ExitOrderADX", 0);}
								if (Position.MarketPosition == MarketPosition.Short) {CloseShort("ExitOrderADX", 0);}
								EEBars = BarsSinceEntryExecution(0, "", 0) + 5;
								Status = 4;
							}
					}
		}

		private void ClosePositionPT()
		{
			if ((Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && PnLPercentage() > PTGPercentage && TrailStatus == false && GetCurrentBid() > PA + PT)
				|| (Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && PnLPercentage() > PTGPercentage && TrailStatus == false && GetCurrentAsk() < PA - PT))
					{
						if(FeeCoveredForTrail() == true)
							{
								TrailStatus = true;
							}
						else
							{
								if (Position.MarketPosition == MarketPosition.Long) {CloseLong("ExitOrderPT", 0);}
								if (Position.MarketPosition == MarketPosition.Short) {CloseShort("ExitOrderPT", 0);}
								EEBars = BarsSinceEntryExecution(0, "", 0) + 5;
								Status = 4;
							}
					}
		}
		
		private void ClosePositionPTFix()
		{
			if ((Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && PnLPercentage() > PTGPercentage && TrailStatus == false && GetCurrentBid() > PA + FixProfit())
				|| (Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && PnLPercentage() > PTGPercentage && TrailStatus == false && GetCurrentAsk() < PA - FixProfit()))
					{
						if(FeeCoveredForTrail() == true)
							{
								TrailStatus = true;
							}
						else
							{
								if (Position.MarketPosition == MarketPosition.Long) {CloseLong("ExitOrderPTFix", 0);}
								if (Position.MarketPosition == MarketPosition.Short) {CloseShort("ExitOrderPTFix", 0);}
								EEBars = BarsSinceEntryExecution(0, "", 0) + 5;
								Status = 4;
							}
					}
		}
		
		private void ClosePosition2PnL()
		{
			if ((Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && TrailStatus == false && PnLPercentage() > (PTGPercentage * 2))
				|| (Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && Status == 3 && FeeCovered() == true && TrailStatus == false && PnLPercentage() > (PTGPercentage * 2)))
					{
						if(FeeCoveredForTrail() == true)
							{
								TrailStatus = true;
							}
						else
							{
								if (Position.MarketPosition == MarketPosition.Long) {CloseLong("ExitOrderPnL", 0);}
								if (Position.MarketPosition == MarketPosition.Short) {CloseShort("ExitOrderPnL", 0);}
								EEBars = BarsSinceEntryExecution(0, "", 0) + 5;
								Status = 4;
							}
					}
		}

		private void ClosePositionTrail()
		{
			if ((Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && TrailStatus == true && FeeCovered() == true && GetCurrentAsk() < PAPTTCompute())
				|| (Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && TrailStatus == true && FeeCovered() == true && GetCurrentBid() > PAPTTCompute()))
					{
						if (Position.MarketPosition == MarketPosition.Long) {CloseLong("ExitOrderTrail", 0);}
						if (Position.MarketPosition == MarketPosition.Short) {CloseShort("ExitOrderTrail", 0);}
						EEBars = BarsSinceEntryExecution(0, "", 0) + 5;
						Status = 4;
					}
		}

		private void ClosePositionEOD()
		{
			DelayThread();
			if (EODMarginMgt() == false && RandomGenerator() > 60)
				{
					if (Position.MarketPosition == MarketPosition.Long && LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar()) {CloseLong("ExitOrderEOD", 0);}
					if (Position.MarketPosition == MarketPosition.Short && LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar()) {CloseShort("ExitOrderEOD", 0);}
					Status = 4;
				}
		}

		private void ClosePositionEOS()
		{
			if (RandomGenerator() > 60 && entryOrder == null && exitOrder == null)
				{
					if (Position.MarketPosition == MarketPosition.Long) {CloseLong("ExitOrderEOS", 0);}
					if (Position.MarketPosition == MarketPosition.Short) {CloseShort("ExitOrderEOS", 0);}
					Status = 4;
				}
		}
		
		private void ClosePositionBP()
		{
			DelayThread();
			if (PositionSizeOnLoss() >=1 && Status == 2 && LossOrder == false && RandomGenerator() > 60)
			{
				if (Position.MarketPosition == MarketPosition.Long && LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar()) {CloseLong("ExitOrderBP", 1);}
				if (Position.MarketPosition == MarketPosition.Short && LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar()) {CloseShort("ExitOrderBP", 1);}
				Status = 4;
			}
		}

		private void ClosePositionProfitBP()
		{
			if (PnLPosition() > 0)
			{
				if (Position.MarketPosition == MarketPosition.Long) { CloseLong("ExitOrderBP", 0); }
				if (Position.MarketPosition == MarketPosition.Short) { CloseShort("ExitOrderBP", 0); }
				Status = 4;
			}

		}

		private void ClosePositionRecovery()
		{
			DelayThread();
			if (PositionSizeOnLoss() >= 1 && PnLPosition() > -(NetLiq() * 0.01) && PnLPosition() < -10.00 && RandomGenerator() > 60)
				{
					if (Position.MarketPosition == MarketPosition.Long && LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar()) {CloseLong("ExitOrderRe", 1);}
					if (Position.MarketPosition == MarketPosition.Short && LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar()) {CloseShort("ExitOrderRe", 1);}
					Status = 4;
				}
		}

		private void ClosePositionEarly()
		{
			DelayThread();
			if (Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && PositionAcceptLossOnLoss() == true && RandomGenerator() > 60
				&& GetCurrentAsk() < MidPriceDailyBar() && Status == 2 && GetCurrentBid() < PASLCompute() && NetProfitL() == true && PositionSizeOnLoss() >= 1
				&& (DM(3).DiMinus[0] > DM(3).DiPlus[0]) && (DM(10).DiMinus[0] > DM(10).DiPlus[0]) && (DM(30).DiMinus[0] > DM(30).DiPlus[0])
				|| Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && PositionAcceptLossOnLoss() == true && RandomGenerator() > 60
				&& GetCurrentBid() > MidPriceDailyBar() && Status == 2 && GetCurrentAsk() > PASLCompute() && NetProfitL() == true && PositionSizeOnLoss() >= 1
				&& (DM(3).DiMinus[0] < DM(3).DiPlus[0]) && (DM(10).DiMinus[0] < DM(10).DiPlus[0]) && (DM(30).DiMinus[0] < DM(30).DiPlus[0]))
					{
						if (Position.MarketPosition == MarketPosition.Long && LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar()) {CloseLong("ExitOrderE", 1);}
						if (Position.MarketPosition == MarketPosition.Short  && LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar()) {CloseShort("ExitOrderE", 1);}
						Status = 4;
					}
		}
		
		private void ClosePositionEarlyOnLoss()
		{
			DelayThread();
			if (Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && Status == 2 && PositionAcceptLossOnLoss() == true && RandomGenerator() > 60
				&& LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar() && LossOrder == false && NetProfitL() == true && PositionSizeOnLoss() >= 1
				|| Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && Status == 2 && PositionAcceptLossOnLoss() == true && RandomGenerator() > 60
				&& LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar() && LossOrder == false && NetProfitL() == true && PositionSizeOnLoss() >= 1)
					{
						if (Position.MarketPosition == MarketPosition.Long  && LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar()) {CloseLong("ExitOrderEOL", 1);}
						if (Position.MarketPosition == MarketPosition.Short  && LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar()) {CloseShort("ExitOrderEOL", 1);}
						Status = 4;
					}
		}

		private void ClosePosition40OnLoss()
		{
			if (Position.MarketPosition == MarketPosition.Long && entryOrder == null && exitOrder == null && Status == 2
				&& PnLPosition() < -500
				&& LossOrder == false
				&& PositionSizeOnLoss() >= 1
				|| Position.MarketPosition == MarketPosition.Short && entryOrder == null && exitOrder == null && Status == 2
				&& PnLPosition() < -500
				&& LossOrder == false
				&& PositionSizeOnLoss() >= 1)
					{
						if (Position.MarketPosition == MarketPosition.Long  && LTTrendStatus() == "short" && GetCurrentAsk() < MidPriceDailyBar()) {CloseLong("ExitOrderEOL40", 1);}
						if (Position.MarketPosition == MarketPosition.Short && LTTrendStatus() == "long" && GetCurrentBid() > MidPriceDailyBar()) {CloseShort("ExitOrderEOL40", 1);}
						Status = 4;
					}
		}

		// Market Data Seq Functions

		private void Seq_Index0_OpenTrades()
		{
			if (TimeFunction() >= StartTime && TimeFunction() < CloseTime)
				{
					if (BuyPower() > BuyPowerMargin)
					{
						if (DayTradeActive() == true && EODMarginMgt() == true && liquid == false) OpenPosition();
						if (DayTradeActive() == true && EODMarginMgt() == true) OpenPositionOptionReady();
					}

					if(BuyPower() > BuyPowerMargin * 0.60)
					{
						if (DayTradeActive() == true && EODMarginMgt() == true && liquid == false) OpenPositionLTN();
						if (DayTradeActive() == true && EODMarginMgt() == true && liquid == false) OpenPositionBSPN();
					}

					if (BuyPower() > BuyPowerMargin * 0.30)
					{
						if (DayTradeActive() == true && EODMarginMgt() == true && PnLPosition() <= -10 && PnLPosition() >= -100 && OpeningAcctValue < NetLiq()
						|| DayTradeActive() == true && EODMarginMgt() == true && Position.Quantity <= 10 && PnLPosition() >= -100) OpenPositionST();

						if (DayTradeActive() == true && EODMarginMgt() == true && PnLPosition() <= -10 && OpeningAcctValue < NetLiq()
						|| DayTradeActive() == true && EODMarginMgt() == true && Position.Quantity <= 10 && OpeningAcctValue < NetLiq()) OpenPositionLT();

						if (DayTradeActive() == true && EODMarginMgt() == true && PnLPosition() <= -10 && OpeningAcctValue < NetLiq()
						|| DayTradeActive() == true && EODMarginMgt() == true && Position.Quantity <= 10 && OpeningAcctValue < NetLiq()) OpenPositionBSP();
					}
					if (DayTradeActive() == false && IntraDayMargin() > 250 && BuyPower() > 100 && Under25KActive == true) OpenPositionUnder25k();
				}
		}

		private void Seq_Index2_OpenTrades()
		{
			if (TimeFunction() >= StartTime && TimeFunction() < CloseTime)
				{
					if (BuyPower() > BuyPowerMargin)
					{
						if (DayTradeActive() == true && EODMarginMgt() == true && liquid == false) OpenPositionBBN();
					}

					if (BuyPower() > BuyPowerMargin * 0.60)
					{
						if (DayTradeActive() == true && EODMarginMgt() == true && PnLPosition() >= -10 && OpeningAcctValue < NetLiq()
						|| DayTradeActive() == true && EODMarginMgt() == true && Position.Quantity <= 10 && PnLPosition() >= -10) OpenPositionBB();
					}

					if (BuyPower() > BuyPowerMargin * 0.30)
					{
						// Do something
					}
				}
		}

		private void Seq_Index0_CloseTrades()
		{
			PASLCompute();
			PAPTTCompute();
			TrailReset();

			if (TimeFunction() >= StartTimePre && TimeFunction() < MarketCloseTimePost - 0001 && DayTradeActive() == true && Position.MarketPosition != MarketPosition.Flat && HoldExit() == false
				|| TimeFunction() >= StartTime && TimeFunction() < MarketCloseTime - 0001 && DayTradeActive() == false && TradedToday == false && Position.MarketPosition != MarketPosition.Flat && HoldExit() == false)
				{
					// Close Position with variable Profit
					ClosePositionTrail();
					ClosePositionADX();
					ClosePositionPT();
					ClosePositionPTFix();
					ClosePosition2PnL();

					// Close Position with Fixed Profit or Negative Profit
					if (BarsSinceEntryExecution(0, "", 0) > EEBars)
						{
							if (PnLPercentage() <= -25.00) ClosePositionEarly();
							if (EODMarginMgt() == false && PnLPercentage() <= -15.00) ClosePositionEarly();
						}
				}
		}

		private void Seq_Index0_RecoveryTrades()
        {
			// Close position at EOS if profitable
			if (DayTradeActive() == true && PnLPosition() > 2.00 && EOSActive() == true && TimeFunction() >= CloseTime - 0005 && TimeFunction() < MarketCloseTime - 0001
				|| DayTradeActive() == false && TradedToday == false && PnLPosition() > 2.00 && EOSActive() == true && TimeFunction() >= CloseTime - 0005 && TimeFunction() < MarketCloseTime - 0001)
			{
				ClosePositionEOS();
			}

			// ManageOnLoss Functions
			if (DayTradeActive() == true && PnLPosition() < -10 && NetProfitL() == true && LTTrendStatus() != null && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime
				|| DayTradeActive() == false && TradedToday == false && PnLPosition() < -10 && NetProfitL() == true && LTTrendStatus() != null && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime)
			{
				OpenPositionOnLoss();
				ClosePositionEarlyOnLoss();
			}

			// Buying Power Management
			if (DayTradeActive() == true && PnLPosition() < -10 && NetProfitL() == false && BuyPower() == 0 && RealizedPL() > -(NetLiq() * 0.01) && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime
				|| DayTradeActive() == false && TradedToday == false && PnLPosition() < -10 && NetProfitL() == false && BuyPower() == 0 && RealizedPL() > -(NetLiq() * 0.01) && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime)
			{
				ClosePositionBP();
			}

			// Buying Power Management - Profitable Trade Closure
			if (DayTradeActive() == true && BuyPower() == 0 && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime && PnLPosition() > 1.00
				|| DayTradeActive() == false && TradedToday == false && BuyPower() == 0 && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime && PnLPosition() > 1.00)
			{
				ClosePositionProfitBP();
			}

			// Close Position Recovery
			if (DayTradeActive() == true && EODMarginMgt() == false && NetProfitL() == false && NetLiq() < OpeningAcctValue && RealizedPL() > -(NetLiq() * 0.01) && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime
				|| DayTradeActive() == false && TradedToday == false && EODMarginMgt() == false && NetProfitL() == false && NetLiq() < OpeningAcctValue && RealizedPL() > -(NetLiq() * 0.01) && TimeFunction() >= StartTime + 30 && TimeFunction() < CloseTime)
			{
				ClosePositionRecovery();
			}
		}

		// Non-Calculation Support Functions

		private void TrailReset()
		{
			if (Position.MarketPosition != MarketPosition.Flat && TrailStatus == true && PnLPercentage() < PTGPercentage) 
			{
				PositionConfiguration();
				TrailStatus = false;
			}
		}

		private void TradeTodayReset()
        {
			if (Position.MarketPosition != MarketPosition.Flat) TradedToday = false;
        }

		private double ReadFileValue(string FileName)
		{
			double value;
			try 
				{
					StreamReader sr = new StreamReader(FileName);
					value = Convert.ToDouble(sr.ReadLine());
					sr.Close();
				}
			catch(IOException e) 
				{
					DelayThread();
					StreamReader sr = new StreamReader(FileName);
					value = Convert.ToDouble(sr.ReadLine());
					sr.Close();
				}
			return value;
		}
		
		private void WriteFileValue(string FileName, double value)
		{	
			try
				{
					DeleteFile(FileName);
					StreamWriter sw = new StreamWriter(FileName);
					sw.WriteLine(value);
					sw.Close();
				}
			catch(IOException e)
				{
					DelayThread();
					DeleteFile(FileName);
					StreamWriter sw = new StreamWriter(FileName);
					sw.WriteLine(value);
					sw.Close();
				}
		}
		
		private void DeleteFile(string FileName)
		{
			try
				{
					if(CheckFile(FileName) == true) File.Delete(FileName);
				}
			catch(IOException e)
				{
					DelayThread();
					if(CheckFile(FileName) == true) File.Delete(FileName);
				}
		}
		
		private bool CheckFile(string FileName)
		{
			try
				{
				return File.Exists(FileName);
				}
			catch(IOException e)
				{
					DelayThread();
					return false;
				}
		}
		
		private void Delay()
		{
			// Random rnd = new Random();
			DateTime DelayTime = DateTime.Now.AddSeconds(RandomGenerator());

			while(DateTime.Now <= DelayTime)
			{
					//code
			}
		}

		private void DelayThread()
		{
			int Value = RandomGenerator() * 1000;
			Thread.Sleep(Value);
		}
		
		private int RandomGenerator()
        {
			Random rnd = new Random();
			return rnd.Next(40, 80);
		}

// Event Functions

		protected override void OnStateChange()
			{
				if (State == State.SetDefaults)
				{
					Description = @"Enter the description for your new custom Strategy here.";
					Name = "SL";
					Calculate = Calculate.OnEachTick;
					EntriesPerDirection = 1;
					EntryHandling = EntryHandling.AllEntries;
					IsExitOnSessionCloseStrategy = false;
					ExitOnSessionCloseSeconds = 30;
					IsFillLimitOnTouch = false;
					MaximumBarsLookBack = MaximumBarsLookBack.TwoHundredFiftySix;
					OrderFillResolution = OrderFillResolution.Standard;
					Slippage = 0;
					IsAdoptAccountPositionAware = true;
					StartBehavior = StartBehavior.AdoptAccountPosition;
					TimeInForce = TimeInForce.Day;
					TraceOrders = false;
					RealtimeErrorHandling = RealtimeErrorHandling.StopCancelCloseIgnoreRejects;
					StopTargetHandling = StopTargetHandling.PerEntryExecution;
					BarsRequiredToTrade = 15;
					IsInstantiatedOnEachOptimizationIteration = false;
					AskBidSpread = 0.06;
					AcctPercentage = 0.10;
					TargetPercentage = 0.001;
					TargetPercentageInterval = 0.001;
					TargetPercentageIntervalDown = 0.001;
					DaysToLoad = 20;
					IsEnabled = true;
					PT = 0;
					STS = 150;
					SPS = 0;
					Status = 0;
					TrailStatus = false;
					entryOrder = null;
					exitOrder = null;
					PTG = 0.00;
					PTGPercentage = 1.0;
					RSISlope = 0.2;
					RSISlopeD = 0.2;
					ADXSlope = 0.1;
					ADXSlopeD = 0.1;
					BBSlope = 2.00;
					EEBars = 15;
					StartTimePre = 0400;
					StartTime = 0930;
					ReviewTime = StartTime - 0002;
					CloseTime = 1540;
					MarketCloseTime = 1600;
					MarketCloseTimePost = 2000;
					RecADXSlope = 0;
					FeeActive = true;
					LossOrder = false;
					LossBorderPercentage = -20;
					EODMarginTarget = 0;
					OpeningAcctValue = 0;
					BuyPowerMargin = 0;
					BPR_TargetPercentage = 0.00;
					BPR_TargetPercentage_Saved = 0.00;
					TradedToday = false;
					Under25KActive = false;
					NetLiqEmulation = false;
					ExcessIntradayMarginEmulation = false;

					// XML file load
					liquid = XmlConfig.liquidate();
					optionTrigger = XmlConfig.optionTrigger();
					buysellPressure = XmlConfig.buysellPressure();
					useCurrency = XmlConfig.useCurrency();
				}
				else if (State == State.Historical)
				{
					// Do Something
				}
				else if (State == State.Configure)
				{
					// BarsInProgress index == 1
					AddDataSeries(Data.BarsPeriodType.Day, 1);
					
					// BarsInProgress index == 2
					AddDataSeries(Data.BarsPeriodType.Minute, 5);
					
					// BarsInProgress index == 3
					AddDataSeries(Data.BarsPeriodType.Minute, 1);

					//AddVolumetric(null, Data.BarsPeriodType.Minute, 1, Data.VolumetricDeltaType.BidAsk, 1);

					//client.BaseAddress = new Uri("http://localhost:64195/");
					//client.DefaultRequestHeaders.Accept.Clear();
					//client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
				}
				else if (State == State.DataLoaded)
				{
					// Daily Attributes	Index 1	
					ATR1 = ATR(Closes[1], 10);
					MACD1 = MACD(Closes[1], 12, 26, 9);
					
					// 5-Minutes Attributes Index 2
					Bollinger5M = Bollinger(Closes[2], 2, 14);

					// 1-Minute Attributes Index 3

					// Daily EMA
					EMAH0 = EMA(Highs[1], 2);
					EMAH1 = EMA(Highs[1], 5);
					EMAH2 = EMA(Highs[1], 10);
					EMAH3 = EMA(Highs[1], 15);

					EMAC0 = EMA(Closes[1], 2);
					EMAC1 = EMA(Closes[1], 5);
					EMAC2 = EMA(Closes[1], 10);
					EMAC3 = EMA(Closes[1], 15);

					EMAL0 = EMA(Lows[1], 2);
					EMAL1 = EMA(Lows[1], 5);
					EMAL2 = EMA(Lows[1], 10);
					EMAL3 = EMA(Lows[1], 15);
					
					// 5-Minute EMA
					EMAH50 = EMA(Highs[2], 2);
					EMAH51 = EMA(Highs[2], 5);
					EMAH52 = EMA(Highs[2], 10);
					EMAH53 = EMA(Highs[2], 15);

					EMAC50 = EMA(Closes[2], 2);
					EMAC51 = EMA(Closes[2], 5);
					EMAC52 = EMA(Closes[2], 10);
					EMAC53 = EMA(Closes[2], 15);

					EMAL50 = EMA(Lows[2], 2);
					EMAL51 = EMA(Lows[2], 5);
					EMAL52 = EMA(Lows[2], 10);
					EMAL53 = EMA(Lows[2], 15);
				}
				else if (State == State.Realtime)
				{
					// Load variables if position is adopted
					if (Position.MarketPosition != MarketPosition.Flat) PositionConfiguration();

					// Set Emulations
					if (NetLiq() == 0 && BuyPower() > 0 || NetLiq() == 0 && CashValue() > 0) NetLiqEmulation = true;
					if (IntraDayMargin() == 0 && BuyPower() > 0 || IntraDayMargin() == 0 && CashValue() > 0) ExcessIntradayMarginEmulation = true; 

					// Set Opening  Account Value Variable
					if (OpeningAcctValue == 0) OpeningAcctValue = NetLiq();

					// Set BuyPower Margin Variable
					if ((BuyPower() * (BuyPowerMarginPercentage())) < (NetLiq() * 0.03))
						{
							BuyPowerMargin = (NetLiq() * 0.03);
						}
					else
						{
							BuyPowerMargin = (BuyPower() * (BuyPowerMarginPercentage()));
						}
				}
			}
		
		protected override void OnBarUpdate()
			{
				if (BarsInProgress == 0)
				{
					if (CurrentBars[0] < 5)
						return;
					
					if (TimeFunction() >= StartTime - 0005 && TimeFunction() < StartTime - 0001)
						TradeTodayReset();

					if (Position.MarketPosition != MarketPosition.Flat && Position.AveragePrice != PositionAccount.AveragePrice && PA != Position.AveragePrice
						|| Position.MarketPosition != MarketPosition.Flat && Position.Quantity != PositionAccount.Quantity && PQ != Position.Quantity)
						PositionConfiguration();

					// Account base adjustments
					TargetAdjustment("Time");
				
					// SetPositionStatus
					SetPositionStatus();

					// Long or Short Trades
					Seq_Index0_OpenTrades();
				
					// Close Trade Calculations Function
					Seq_Index0_CloseTrades();

					// Maintanence Account Functions
					Seq_Index0_RecoveryTrades();

					// BSP Reset
					BSPCount("reset");
				}
				else if (BarsInProgress == 1)
				{
					if (CurrentBars[1] < 5)
						return;

					// Do something index 1
				}
				else if (BarsInProgress == 2)
				{
					if (CurrentBars[2] < 5)
						return;

					// SetPositionStatus
					SetPositionStatus();

					// Long or Short Trades
					Seq_Index2_OpenTrades();
				}
			}
		
		protected override void OnOrderUpdate(Order order, double limitPrice, double stopPrice, int quantity, int filled, double averageFillPrice, OrderState orderState, DateTime time, ErrorCode error, string nativeError)
			{
			// Control
			if (order.Name == "BuyOrder" || order.Name == "ShortOrder" || order.Name == "BuyOrderLoss" || order.Name == "ShortOrderLoss"
				|| order.Name == "BuyOrderST" || order.Name == "ShortOrderST" || order.Name == "BuyOrderLT" || order.Name == "BuyOrderLTOption" 
				|| order.Name == "ShortOrderLT" || order.Name == "ShortOrderLTOption"
				|| order.Name == "BuyOrderBB" || order.Name == "ShortOrderBB" || order.Name == "BuyOrderBBN" || order.Name == "ShortOrderBBN" 
				|| order.Name == "BuyOrderLTN" || order.Name == "ShortOrderLTN" || order.Name == "BuyOrder25K" || order.Name == "ShortOrder25K")
				  {
			      	entryOrder = order;
			 		PA = 0;
					PQ = 0;
					PT = 0;
					PTG = 0;
					PASL = 0;
					PAPTT = 0;
					RecADXSlope = 0;
					RecProfitTrigger = 0;
					TrailStatus = false;
					TradedToday = false;
				  }

			if (order.Name == "ExitOrderADX" || order.Name == "ExitOrderPT" || order.Name == "ExitOrderTrail" || order.Name == "ExitOrderRe"
				|| order.Name == "ExitOrderEOD" || order.Name == "ExitOrderEOS" || order.Name == "ExitOrderE" || order.Name == "ExitOrderEOL" || order.Name == "ExitOrderBP"
				|| order.Name == "ExitOrderEOL40" || order.Name == "ExitOrderPTFix" || order.Name == "ExitOrderPnL")
				  {
			      	exitOrder = order;
				  }

			// Command
			if (entryOrder != null && entryOrder == order)
					{
					//Print(order.ToString());
			     	if (order.OrderState == OrderState.Filled)
				  		{
                            if (order.Name == "BuyOrder" || order.Name == "BuyOrderLTN" || order.Name == "BuyOrderBSPN" || order.Name == "BuyOrderBBN")
                            {
                                PA = averageFillPrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA - PT*2;
								PAPTT = PA + TPATR();
								RecADXSlope = Slope(ADX(High,10),10,0);
								RecProfitTrigger = PA + PT;
								LossOrder = false;
								TradedToday = true;
                            }
                            if (order.Name == "ShortOrder" || order.Name == "ShortOrderLTN" || order.Name == "ShortOrderBSPN" || order.Name == "ShortOrderBBN")
                            {
                                PA = averageFillPrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA + PT*2;
								PAPTT = PA - TPATR();
								RecADXSlope = Slope(ADX(Low,10),10,0);
								RecProfitTrigger = PA - PT;
								LossOrder = false;
								TradedToday = true;
                            }
							if (order.Name == "BuyOrderST" || order.Name == "BuyOrderLT" || order.Name == "BuyOrderBSP" || order.Name == "BuyOrderLTOption" || order.Name == "BuyOrderBB" || order.Name == "BuyOrderLoss" || order.Name == "BuyOrder25K")
                            {
                                PA = Position.AveragePrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA - PT*2;
								PAPTT = PA + TPATR();
								RecADXSlope = Slope(ADX(High,10),10,0);
								RecProfitTrigger = PA + PT;
								LossOrder = true;
								TradedToday = true;
                            }
                            if (order.Name == "ShortOrderST" || order.Name == "ShortOrderLT" || order.Name == "ShortOrderBSP" || order.Name == "ShortOrderLTOption" || order.Name == "ShortOrderBB" || order.Name == "ShortOrderLoss" || order.Name == "ShortOrder25K")
                            {
                                PA = Position.AveragePrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA + PT*2;
								PAPTT = PA - TPATR();
								RecADXSlope = Slope(ADX(Low,10),10,0);
								RecProfitTrigger = PA - PT;
								LossOrder = true;
								TradedToday = true;
                            }
						entryOrder = null;
				  		}
					if (order.OrderState == OrderState.PartFilled)
						{
							if (order.Name == "BuyOrder" || order.Name == "BuyOrderLTN" || order.Name == "BuyOrderBSPN" || order.Name == "BuyOrderBBN")
                            {
                                PA = averageFillPrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA - PT*2;
								PAPTT = PA + TPATR();
								RecADXSlope = Slope(ADX(High,10),10,0);
								RecProfitTrigger = PA + PT;
								LossOrder = false;
								TradedToday = true;
                            }
                            if (order.Name == "ShortOrder" || order.Name == "ShortOrderLTN" || order.Name == "ShortOrderBSPN" || order.Name == "ShortOrderBBN")
                            {
                                PA = averageFillPrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA + PT*2;
								PAPTT = PA - TPATR();
								RecADXSlope = Slope(ADX(Low,10),10,0);
								RecProfitTrigger = PA - PT;
								LossOrder = false;
								TradedToday = true;
                            }
							if (order.Name == "BuyOrderST" || order.Name == "BuyOrderLT" || order.Name == "BuyOrderBSP" || order.Name == "BuyOrderLTOption" || order.Name == "BuyOrderBB" || order.Name == "BuyOrderLoss" || order.Name == "BuyOrder25K")
                            {
                                PA = Position.AveragePrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA - PT*2;
								PAPTT = PA + TPATR();
								RecADXSlope = Slope(ADX(High,10),10,0);
								RecProfitTrigger = PA + PT;
								LossOrder = false;
								TradedToday = true;
                            }
                            if (order.Name == "ShortOrderST" || order.Name == "ShortOrderLT" || order.Name == "ShortOrderBSP" || order.Name == "ShortOrderLTOption" || order.Name == "ShortOrderBB" || order.Name == "ShortOrderLoss" || order.Name == "ShortOrder25K")
                            {
                                PA = Position.AveragePrice;
								PQ = quantity;
								PT = PATR();
                                PASL = PA + PT*2;
								PAPTT = PA - TPATR();
								RecADXSlope = Slope(ADX(Low,10),10,0);
								RecProfitTrigger = PA - PT;
								LossOrder = false;
								TradedToday = true;
                            }
						}
					if (order.OrderState == OrderState.Cancelled || order.OrderState == OrderState.Rejected)
				  		{
							if (order.Name == "BuyOrderST" 	|| order.Name == "BuyOrderLT" || order.Name == "BuyOrderBSP" || order.Name == "BuyOrderLTOption"
								|| order.Name == "BuyOrderLTN" || order.Name == "BuyOrderBSPN" || order.Name == "BuyOrderBB" || order.Name == "BuyOrderBBN"
								|| order.Name == "BuyOrderLoss" || order.Name == "BuyOrder25K")
                            {
                                PA = PositionAccount.AveragePrice;
								PQ = PositionAccount.Quantity;
								PT = PATR();
                                PASL = PA - PT*2;
								PAPTT = PA + TPATR();
								RecADXSlope = Slope(ADX(High,10),10,0);
								RecProfitTrigger = PA + PT;
								LossOrder = false;
								TradedToday = false;
                            }
							
                            if (order.Name == "ShortOrderST" || order.Name == "ShortOrderLT" || order.Name == "ShortOrderBSP" || order.Name == "ShortOrderLTOption" 
								|| order.Name == "ShortOrderLTN" || order.Name == "ShortOrderBSPN" || order.Name == "ShortOrderBB" || order.Name == "ShortOrderBBN"
								|| order.Name == "ShortOrderLoss" || order.Name == "ShortOrder25K")
                            {
                                PA = PositionAccount.AveragePrice;
								PQ = PositionAccount.Quantity;
								PT = PATR();
                                PASL = PA + PT*2;
								PAPTT = PA - TPATR();
								RecADXSlope = Slope(ADX(Low,10),10,0);
								RecProfitTrigger = PA - PT;
								LossOrder = false;
								TradedToday = false;
                            }
							entryOrder = null;
				  		}
					}

			if (exitOrder != null && exitOrder == order)
				{
					if (order.OrderState == OrderState.Filled)
					{
						if (order.Name == "ExitOrderEOL" || order.Name == "ExitOrderBP" || order.Name == "ExitOrderEOL40" || order.Name == "ExitOrderE") 
						{
							LossOrder = true;
						}
						if (order.Name == "ExitOrderTrail")
						{
							TargetAdjustment("TrailStop");
						}
						exitOrder = null;
					}

					if (order.OrderState == OrderState.PartFilled)
					{
						if (Position.MarketPosition != MarketPosition.Flat) 
						{
							PositionConfiguration();
							Status = 2;
						}
						if (order.Name == "ExitOrderTrail")
						{
							TargetAdjustment("TrailStop");
						}
						exitOrder = null;
					}
					
					if (order.OrderState == OrderState.Cancelled || order.OrderState == OrderState.Rejected)
					{
						if (Position.MarketPosition != MarketPosition.Flat) 
						{
							Status = 2;
						}
						exitOrder = null;
					}
				}

			if (order.OrderState == OrderState.Rejected)
				{
					if (Position.MarketPosition != MarketPosition.Flat)
					{
						PositionConfiguration();
					}
					exitOrder = null;
				}
			}
		
		private void OnAccountStatusUpdate(object sender, AccountStatusEventArgs e)
			{
				// Do something with the account status update
			}

		private void OnAccountItemUpdate(object sender, AccountItemEventArgs e)
			{
				// Do something with the account item update
			}
 
		private void OnPositionUpdate(object sender, PositionEventArgs e)
			{
				// Do something
			}
		
		private void OnExecutionUpdate(object sender, PositionEventArgs e)
			{
				// Do Something
			}

		protected override void OnMarketData(MarketDataEventArgs marketDataUpdate)
			{
				if (marketDataUpdate.MarketDataType == MarketDataType.Ask)
					{
						SetPositionStatus();
						Seq_Index0_OpenTrades();
						Seq_Index2_OpenTrades();
						if (Position.MarketPosition != MarketPosition.Flat) Seq_Index0_CloseTrades();
						BSPCount("ask");
					}
				else if (marketDataUpdate.MarketDataType == MarketDataType.Bid)
					{
						SetPositionStatus();
						Seq_Index0_OpenTrades();
						Seq_Index2_OpenTrades();
						if (Position.MarketPosition != MarketPosition.Flat) Seq_Index0_CloseTrades();
						BSPCount("bid");
					}
			}

// Tunable Variables

		#region Properties
//		[NinjaScriptProperty]
//		[Range(1, int.MaxValue)]
//		[Display(Name="Trail Profit Ticks", Order=1, GroupName="Parameters")]
//		public int SPS
//		{ get; set; }
		
//		[NinjaScriptProperty]
//		[Display(Name="Position Test", Order=1, GroupName="Parameters")]
//		public bool PositionTest
//		{ get; set; }
		
//		[NinjaScriptProperty]
//		[Range(1, int.MaxValue)]
//		[Display(Name="Trail Stop Ticks", Order=1, GroupName="Parameters")]
//		public int STS
//		{ get; set; }
        #endregion
	}
}