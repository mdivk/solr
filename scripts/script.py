from optparse import OptionParser
import gzip
import json
import bz2
import os
from pathlib import Path
import re

parser = OptionParser()
parser.add_option("-p", "--path", dest="zip",
                  help="zip file path", metavar="PATH")
parser.add_option("-o", "--output", dest="json",
                  help="json file path", metavar="OUTPUT")
parser.add_option("-q", "--quiet",
                  action="store_false", dest="verbose", default=True,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()

res = options.json

tags = [str(i) for i in range(1, 957)]
values = ['Account',
 'AdvId',
 'AdvRefID',
 'AdvSide',
 'AdvTransType',
 'AvgPx',
 'BeginSeqNo',
 'BeginString',
 'BodyLength',
 'CheckSum',
 'ClOrdID',
 'Commission',
 'CommType',
 'CumQty',
 'Currency',
 'EndSeqNo',
 'ExecID',
 'ExecInst',
 'ExecRefID',
 'ExecTransType',
 'HandlInst',
 'SecurityIDSource',
 'IOIID',
 'IOIOthSvc  (no longer used)',
 'IOIQltyInd',
 'IOIRefID',
 'IOIQty',
 'IOITransType',
 'LastCapacity',
 'LastMkt',
 'LastPx',
 'LastQty',
 'NoLinesOfText',
 'MsgSeqNum',
 'MsgType',
 'NewSeqNo',
 'OrderID',
 'OrderQty',
 'OrdStatus',
 'OrdType',
 'OrigClOrdID',
 'OrigTime',
 'PossDupFlag',
 'Price',
 'RefSeqNum',
 'RelatdSym  (no longer used)',
 'Rule80A(No Longer Used)',
 'SecurityID',
 'SenderCompID',
 'SenderSubID',
 'SendingDate  (no longer used)',
 'SendingTime',
 'Quantity',
 'Side',
 'Symbol',
 'TargetCompID',
 'TargetSubID',
 'Text',
 'TimeInForce',
 'TransactTime',
 'Urgency',
 'ValidUntilTime',
 'SettlType',
 'SettlDate',
 'SymbolSfx',
 'ListID',
 'ListSeqNo',
 'TotNoOrders',
 'ListExecInst',
 'AllocID',
 'AllocTransType',
 'RefAllocID',
 'NoOrders',
 'AvgPxPrecision',
 'TradeDate',
 'ExecBroker',
 'PositionEffect',
 'NoAllocs',
 'AllocAccount',
 'AllocQty',
 'ProcessCode',
 'NoRpts',
 'RptSeq',
 'CxlQty',
 'NoDlvyInst',
 'DlvyInst',
 'AllocStatus',
 'AllocRejCode',
 'Signature',
 'SecureDataLen',
 'SecureData',
 'BrokerOfCredit',
 'SignatureLength',
 'EmailType',
 'RawDataLength',
 'RawData',
 'PossResend',
 'EncryptMethod',
 'StopPx',
 'ExDestination',
 'CxlRejReason',
 'OrdRejReason',
 'IOIQualifier',
 'WaveNo',
 'Issuer',
 'SecurityDesc',
 'HeartBtInt',
 'ClientID',
 'MinQty',
 'MaxFloor',
 'TestReqID',
 'ReportToExch',
 'LocateReqd',
 'OnBehalfOfCompID',
 'OnBehalfOfSubID',
 'QuoteID',
 'NetMoney',
 'SettlCurrAmt',
 'SettlCurrency',
 'ForexReq',
 'OrigSendingTime',
 'GapFillFlag',
 'NoExecs',
 'CxlType',
 'ExpireTime',
 'DKReason',
 'DeliverToCompID',
 'DeliverToSubID',
 'IOINaturalFlag',
 'QuoteReqID',
 'BidPx',
 'OfferPx',
 'BidSize',
 'OfferSize',
 'NoMiscFees',
 'MiscFeeAmt',
 'MiscFeeCurr',
 'MiscFeeType',
 'PrevClosePx',
 'ResetSeqNumFlag',
 'SenderLocationID',
 'TargetLocationID',
 'OnBehalfOfLocationID',
 'DeliverToLocationID',
 'NoRelatedSym',
 'Subject',
 'Headline',
 'URLLink',
 'ExecType',
 'LeavesQty',
 'CashOrderQty',
 'AllocAvgPx',
 'AllocNetMoney',
 'SettlCurrFxRate',
 'SettlCurrFxRateCalc',
 'NumDaysInterest',
 'AccruedInterestRate',
 'AccruedInterestAmt',
 'SettlInstMode',
 'AllocText',
 'SettlInstID',
 'SettlInstTransType',
 'EmailThreadID',
 'SettlInstSource',
 'SettlLocation',
 'SecurityType',
 'EffectiveTime',
 'StandInstDbType',
 'StandInstDbName',
 'StandInstDbID',
 'SettlDeliveryType',
 'SettlDepositoryCode ',
 'SettlBrkrCode ',
 'SettlInstCode ',
 'SecuritySettlAgentName',
 'SecuritySettlAgentCode',
 'SecuritySettlAgentAcctNum',
 'SecuritySettlAgentAcctName',
 'SecuritySettlAgentContactName',
 'SecuritySettlAgentContactPhone',
 'CashSettlAgentName',
 'CashSettlAgentCode',
 'CashSettlAgentAcctNum',
 'CashSettlAgentAcctName',
 'CashSettlAgentContactName',
 'CashSettlAgentContactPhone',
 'BidSpotRate',
 'BidForwardPoints',
 'OfferSpotRate',
 'OfferForwardPoints',
 'OrderQty2',
 'SettlDate2',
 'LastSpotRate',
 'LastForwardPoints',
 'AllocLinkID',
 'AllocLinkType',
 'SecondaryOrderID',
 'NoIOIQualifiers',
 'MaturityMonthYear',
 'PutOrCall',
 'StrikePrice',
 'CoveredOrUncovered',
 'CustomerOrFirm',
 'MaturityDay',
 'OptAttribute',
 'SecurityExchange',
 'NotifyBrokerOfCredit',
 'AllocHandlInst',
 'MaxShow',
 'PegOffsetValue',
 'XmlDataLen',
 'XmlData',
 'SettlInstRefID',
 'NoRoutingIDs',
 'RoutingType',
 'RoutingID',
 'Spread',
 'Benchmark',
 'BenchmarkCurveCurrency',
 'BenchmarkCurveName',
 'BenchmarkCurvePoint',
 'CouponRate',
 'CouponPaymentDate',
 'IssueDate',
 'RepurchaseTerm',
 'RepurchaseRate',
 'Factor',
 'TradeOriginationDate',
 'ExDate',
 'ContractMultiplier',
 'NoStipulations',
 'StipulationType',
 'StipulationValue',
 'YieldType',
 'Yield',
 'TotalTakedown',
 'Concession',
 'RepoCollateralSecurityType',
 'RedemptionDate',
 'UnderlyingCouponPaymentDate',
 'UnderlyingIssueDate',
 'UnderlyingRepoCollateralSecurityType',
 'UnderlyingRepurchaseTerm',
 'UnderlyingRepurchaseRate',
 'UnderlyingFactor',
 'UnderlyingRedemptionDate',
 'LegCouponPaymentDate',
 'LegIssueDate',
 'LegRepoCollateralSecurityType',
 'LegRepurchaseTerm',
 'LegRepurchaseRate',
 'LegFactor',
 'LegRedemptionDate',
 'CreditRating',
 'UnderlyingCreditRating',
 'LegCreditRating',
 'TradedFlatSwitch',
 'BasisFeatureDate',
 'BasisFeaturePrice',
 'MDReqID',
 'SubscriptionRequestType',
 'MarketDepth',
 'MDUpdateType',
 'AggregatedBook',
 'NoMDEntryTypes',
 'NoMDEntries',
 'MDEntryType',
 'MDEntryPx',
 'MDEntrySize',
 'MDEntryDate',
 'MDEntryTime',
 'TickDirection',
 'MDMkt',
 'QuoteCondition',
 'TradeCondition',
 'MDEntryID',
 'MDUpdateAction',
 'MDEntryRefID',
 'MDReqRejReason',
 'MDEntryOriginator',
 'LocationID',
 'DeskID',
 'DeleteReason',
 'OpenCloseSettlFlag',
 'SellerDays',
 'MDEntryBuyer',
 'MDEntrySeller',
 'MDEntryPositionNo',
 'FinancialStatus',
 'CorporateAction',
 'DefBidSize',
 'DefOfferSize',
 'NoQuoteEntries',
 'NoQuoteSets',
 'QuoteStatus',
 'QuoteCancelType',
 'QuoteEntryID',
 'QuoteRejectReason',
 'QuoteResponseLevel',
 'QuoteSetID',
 'QuoteRequestType',
 'TotNoQuoteEntries',
 'UnderlyingSecurityIDSource',
 'UnderlyingIssuer',
 'UnderlyingSecurityDesc',
 'UnderlyingSecurityExchange',
 'UnderlyingSecurityID',
 'UnderlyingSecurityType',
 'UnderlyingSymbol',
 'UnderlyingSymbolSfx',
 'UnderlyingMaturityMonthYear',
 'UnderlyingMaturityDay',
 'UnderlyingPutOrCall',
 'UnderlyingStrikePrice',
 'UnderlyingOptAttribute',
 'UnderlyingCurrency',
 'RatioQty',
 'SecurityReqID',
 'SecurityRequestType',
 'SecurityResponseID',
 'SecurityResponseType',
 'SecurityStatusReqID',
 'UnsolicitedIndicator',
 'SecurityTradingStatus',
 'HaltReason',
 'InViewOfCommon',
 'DueToRelated',
 'BuyVolume',
 'SellVolume',
 'HighPx',
 'LowPx',
 'Adjustment',
 'TradSesReqID',
 'TradingSessionID',
 'ContraTrader',
 'TradSesMethod',
 'TradSesMode',
 'TradSesStatus',
 'TradSesStartTime',
 'TradSesOpenTime',
 'TradSesPreCloseTime',
 'TradSesCloseTime',
 'TradSesEndTime',
 'NumberOfOrders',
 'MessageEncoding',
 'EncodedIssuerLen',
 'EncodedIssuer',
 'EncodedSecurityDescLen',
 'EncodedSecurityDesc',
 'EncodedListExecInstLen',
 'EncodedListExecInst',
 'EncodedTextLen',
 'EncodedText',
 'EncodedSubjectLen',
 'EncodedSubject',
 'EncodedHeadlineLen',
 'EncodedHeadline',
 'EncodedAllocTextLen',
 'EncodedAllocText',
 'EncodedUnderlyingIssuerLen',
 'EncodedUnderlyingIssuer',
 'EncodedUnderlyingSecurityDescLen',
 'EncodedUnderlyingSecurityDesc',
 'AllocPrice',
 'QuoteSetValidUntilTime',
 'QuoteEntryRejectReason',
 'LastMsgSeqNumProcessed',
 'OnBehalfOfSendingTime',
 'RefTagID',
 'RefMsgType',
 'SessionRejectReason',
 'BidRequestTransType',
 'ContraBroker',
 'ComplianceID',
 'SolicitedFlag',
 'ExecRestatementReason',
 'BusinessRejectRefID',
 'BusinessRejectReason',
 'GrossTradeAmt',
 'NoContraBrokers',
 'MaxMessageSize',
 'NoMsgTypes',
 'MsgDirection',
 'NoTradingSessions',
 'TotalVolumeTraded',
 'DiscretionInst',
 'DiscretionOffsetValue',
 'BidID',
 'ClientBidID',
 'ListName',
 'TotNoRelatedSym',
 'BidType',
 'NumTickets',
 'SideValue1',
 'SideValue2',
 'NoBidDescriptors',
 'BidDescriptorType',
 'BidDescriptor',
 'SideValueInd',
 'LiquidityPctLow',
 'LiquidityPctHigh',
 'LiquidityValue',
 'EFPTrackingError',
 'FairValue',
 'OutsideIndexPct',
 'ValueOfFutures',
 'LiquidityIndType',
 'WtAverageLiquidity',
 'ExchangeForPhysical',
 'OutMainCntryUIndex',
 'CrossPercent',
 'ProgRptReqs',
 'ProgPeriodInterval',
 'IncTaxInd',
 'NumBidders',
 'BidTradeType',
 'BasisPxType',
 'NoBidComponents',
 'Country',
 'TotNoStrikes',
 'PriceType',
 'DayOrderQty',
 'DayCumQty',
 'DayAvgPx',
 'GTBookingInst',
 'NoStrikes',
 'ListStatusType',
 'NetGrossInd',
 'ListOrderStatus',
 'ExpireDate',
 'ListExecInstType',
 'CxlRejResponseTo',
 'UnderlyingCouponRate',
 'UnderlyingContractMultiplier',
 'ContraTradeQty',
 'ContraTradeTime',
 'ClearingFirm',
 'ClearingAccount',
 'LiquidityNumSecurities',
 'MultiLegReportingType',
 'StrikeTime',
 'ListStatusText',
 'EncodedListStatusTextLen',
 'EncodedListStatusText',
 'PartyIDSource',
 'PartyID',
 'TotalVolumeTradedDate',
 'TotalVolumeTraded Time',
 'NetChgPrevDay',
 'PartyRole',
 'NoPartyIDs',
 'NoSecurityAltID',
 'SecurityAltID',
 'SecurityAltIDSource',
 'NoUnderlyingSecurityAltID',
 'UnderlyingSecurityAltID',
 'UnderlyingSecurityAltIDSource',
 'Product',
 'CFICode',
 'UnderlyingProduct',
 'UnderlyingCFICode',
 'TestMessageIndicator',
 'QuantityType',
 'BookingRefID',
 'IndividualAllocID',
 'RoundingDirection',
 'RoundingModulus',
 'CountryOfIssue',
 'StateOrProvinceOfIssue',
 'LocaleOfIssue',
 'NoRegistDtls',
 'MailingDtls',
 'InvestorCountryOfResidence',
 'PaymentRef',
 'DistribPaymentMethod',
 'CashDistribCurr',
 'CommCurrency',
 'CancellationRights',
 'MoneyLaunderingStatus',
 'MailingInst',
 'TransBkdTime',
 'ExecPriceType',
 'ExecPriceAdjustment',
 'DateOfBirth',
 'TradeReportTransType',
 'CardHolderName',
 'CardNumber',
 'CardExpDate',
 'CardIssNum',
 'PaymentMethod',
 'RegistAcctType',
 'Designation',
 'TaxAdvantageType',
 'RegistRejReasonText',
 'FundRenewWaiv',
 'CashDistribAgentName',
 'CashDistribAgentCode',
 'CashDistribAgentAcctNumber',
 'CashDistribPayRef',
 'CashDistribAgentAcctName',
 'CardStartDate',
 'PaymentDate',
 'PaymentRemitterID',
 'RegistStatus',
 'RegistRejReasonCode',
 'RegistRefID',
 'RegistDtls',
 'NoDistribInsts',
 'RegistEmail',
 'DistribPercentage',
 'RegistID',
 'RegistTransType',
 'ExecValuationPoint',
 'OrderPercent',
 'OwnershipType',
 'NoContAmts',
 'ContAmtType',
 'ContAmtValue',
 'ContAmtCurr',
 'OwnerType',
 'PartySubID',
 'NestedPartyID',
 'NestedPartyIDSource',
 'SecondaryClOrdID',
 'SecondaryExecID',
 'OrderCapacity',
 'OrderRestrictions',
 'MassCancelRequestType',
 'MassCancelResponse',
 'MassCancelRejectReason',
 'TotalAffectedOrders',
 'NoAffectedOrders',
 'AffectedOrderID',
 'AffectedSecondaryOrderID',
 'QuoteType',
 'NestedPartyRole',
 'NoNestedPartyIDs',
 'TotalAccruedInterestAmt',
 'MaturityDate',
 'UnderlyingMaturityDate',
 'InstrRegistry',
 'CashMargin',
 'NestedPartySubID',
 'Scope',
 'MDImplicitDelete',
 'CrossID',
 'CrossType',
 'CrossPrioritization',
 'OrigCrossID',
 'NoSides',
 'Username',
 'Password',
 'NoLegs',
 'LegCurrency',
 'TotNoSecurityTypes',
 'NoSecurityTypes',
 'SecurityListRequestType',
 'SecurityRequestResult',
 'RoundLot',
 'MinTradeVol',
 'MultiLegRptTypeReq',
 'LegPositionEffect',
 'LegCoveredOrUncovered',
 'LegPrice',
 'TradSesStatusRejReason',
 'TradeRequestID',
 'TradeRequestType',
 'PreviouslyReported',
 'TradeReportID',
 'TradeReportRefID',
 'MatchStatus',
 'MatchType',
 'OddLot',
 'NoClearingInstructions',
 'ClearingInstruction',
 'TradeInputSource',
 'TradeInputDevice',
 'NoDates',
 'AccountType',
 'CustOrderCapacity',
 'ClOrdLinkID',
 'MassStatusReqID',
 'MassStatusReqType',
 'OrigOrdModTime',
 'LegSettlType',
 'LegSettlDate',
 'DayBookingInst',
 'BookingUnit',
 'PreallocMethod',
 'UnderlyingCountryOfIssue',
 'UnderlyingStateOrProvinceOfIssue',
 'UnderlyingLocaleOfIssue',
 'UnderlyingInstrRegistry',
 'LegCountryOfIssue',
 'LegStateOrProvinceOfIssue',
 'LegLocaleOfIssue',
 'LegInstrRegistry',
 'LegSymbol',
 'LegSymbolSfx',
 'LegSecurityID',
 'LegSecurityIDSource',
 'NoLegSecurityAltID',
 'LegSecurityAltID',
 'LegSecurityAltIDSource',
 'LegProduct',
 'LegCFICode',
 'LegSecurityType',
 'LegMaturityMonthYear',
 'LegMaturityDate',
 'LegStrikePrice',
 'LegOptAttribute',
 'LegContractMultiplier',
 'LegCouponRate',
 'LegSecurityExchange',
 'LegIssuer',
 'EncodedLegIssuerLen',
 'EncodedLegIssuer',
 'LegSecurityDesc',
 'EncodedLegSecurityDescLen',
 'EncodedLegSecurityDesc',
 'LegRatioQty',
 'LegSide',
 'TradingSessionSubID',
 'AllocType',
 'NoHops',
 'HopCompID',
 'HopSendingTime',
 'HopRefID',
 'MidPx',
 'BidYield',
 'MidYield',
 'OfferYield',
 'ClearingFeeIndicator',
 'WorkingIndicator',
 'LegLastPx',
 'PriorityIndicator',
 'PriceImprovement',
 'Price2',
 'LastForwardPoints2',
 'BidForwardPoints2',
 'OfferForwardPoints2',
 'RFQReqID',
 'MktBidPx',
 'MktOfferPx',
 'MinBidSize',
 'MinOfferSize',
 'QuoteStatusReqID',
 'LegalConfirm',
 'UnderlyingLastPx',
 'UnderlyingLastQty',
 'SecDefStatus',
 'LegRefID',
 'ContraLegRefID',
 'SettlCurrBidFxRate',
 'SettlCurrOfferFxRate',
 'QuoteRequestRejectReason',
 'SideComplianceID',
 'AcctIDSource',
 'AllocAcctIDSource',
 'BenchmarkPrice',
 'BenchmarkPriceType',
 'ConfirmID',
 'ConfirmStatus',
 'ConfirmTransType',
 'ContractSettlMonth',
 'DeliveryForm',
 'LastParPx',
 'NoLegAllocs',
 'LegAllocAccount',
 'LegIndividualAllocID',
 'LegAllocQty',
 'LegAllocAcctIDSource',
 'LegSettlCurrency',
 'LegBenchmarkCurveCurrency',
 'LegBenchmarkCurveName',
 'LegBenchmarkCurvePoint',
 'LegBenchmarkPrice',
 'LegBenchmarkPriceType',
 'LegBidPx',
 'LegIOIQty',
 'NoLegStipulations',
 'LegOfferPx',
 'LegOrderQty',
 'LegPriceType',
 'LegQty',
 'LegStipulationType',
 'LegStipulationValue',
 'LegSwapType',
 'Pool',
 'QuotePriceType',
 'QuoteRespID',
 'QuoteRespType',
 'QuoteQualifier',
 'YieldRedemptionDate',
 'YieldRedemptionPrice',
 'YieldRedemptionPriceType',
 'BenchmarkSecurityID',
 'ReversalIndicator',
 'YieldCalcDate',
 'NoPositions',
 'PosType',
 'LongQty',
 'ShortQty',
 'PosQtyStatus',
 'PosAmtType',
 'PosAmt',
 'PosTransType',
 'PosReqID',
 'NoUnderlyings',
 'PosMaintAction',
 'OrigPosReqRefID',
 'PosMaintRptRefID',
 'ClearingBusinessDate',
 'SettlSessID',
 'SettlSessSubID',
 'AdjustmentType',
 'ContraryInstructionIndicator',
 'PriorSpreadIndicator',
 'PosMaintRptID',
 'PosMaintStatus',
 'PosMaintResult',
 'PosReqType',
 'ResponseTransportType',
 'ResponseDestination',
 'TotalNumPosReports',
 'PosReqResult',
 'PosReqStatus',
 'SettlPrice',
 'SettlPriceType',
 'UnderlyingSettlPrice',
 'UnderlyingSettlPriceType',
 'PriorSettlPrice',
 'NoQuoteQualifiers',
 'AllocSettlCurrency',
 'AllocSettlCurrAmt',
 'InterestAtMaturity',
 'LegDatedDate',
 'LegPool',
 'AllocInterestAtMaturity',
 'AllocAccruedInterestAmt',
 'DeliveryDate',
 'AssignmentMethod',
 'AssignmentUnit',
 'OpenInterest',
 'ExerciseMethod',
 'TotNumTradeReports',
 'TradeRequestResult',
 'TradeRequestStatus',
 'TradeReportRejectReason',
 'SideMultiLegReportingType',
 'NoPosAmt',
 'AutoAcceptIndicator',
 'AllocReportID',
 'NoNested2PartyIDs',
 'Nested2PartyID',
 'Nested2PartyIDSource',
 'Nested2PartyRole',
 'Nested2PartySubID',
 'BenchmarkSecurityIDSource',
 'SecuritySubType',
 'UnderlyingSecuritySubType',
 'LegSecuritySubType',
 'AllowableOneSidednessPct',
 'AllowableOneSidednessValue',
 'AllowableOneSidednessCurr',
 'NoTrdRegTimestamps',
 'TrdRegTimestamp',
 'TrdRegTimestampType',
 'TrdRegTimestampOrigin',
 'ConfirmRefID',
 'ConfirmType',
 'ConfirmRejReason',
 'BookingType',
 'IndividualAllocRejCode',
 'SettlInstMsgID',
 'NoSettlInst',
 'LastUpdateTime',
 'AllocSettlInstType',
 'NoSettlPartyIDs',
 'SettlPartyID',
 'SettlPartyIDSource',
 'SettlPartyRole',
 'SettlPartySubID',
 'SettlPartySubIDType',
 'DlvyInstType',
 'TerminationType',
 'NextExpectedMsgSeqNum',
 'OrdStatusReqID',
 'SettlInstReqID',
 'SettlInstReqRejCode',
 'SecondaryAllocID',
 'AllocReportType',
 'AllocReportRefID',
 'AllocCancReplaceReason',
 'CopyMsgIndicator',
 'AllocAccountType',
 'OrderAvgPx',
 'OrderBookingQty',
 'NoSettlPartySubIDs',
 'NoPartySubIDs',
 'PartySubIDType',
 'NoNestedPartySubIDs',
 'NestedPartySubIDType',
 'NoNested2PartySubIDs',
 'Nested2PartySubIDType',
 'AllocIntermedReqType',
 'UnderlyingPx',
 'PriceDelta',
 'ApplQueueMax',
 'ApplQueueDepth',
 'ApplQueueResolution',
 'ApplQueueAction',
 'NoAltMDSource',
 'AltMDSourceID',
 'SecondaryTradeReportID',
 'AvgPxIndicator',
 'TradeLinkID',
 'OrderInputDevice',
 'UnderlyingTradingSessionID',
 'UnderlyingTradingSessionSubID',
 'TradeLegRefID',
 'ExchangeRule',
 'TradeAllocIndicator',
 'ExpirationCycle',
 'TrdType',
 'TrdSubType',
 'TransferReason',
 'AsgnReqID',
 'TotNumAssignmentReports',
 'AsgnRptID',
 'ThresholdAmount',
 'PegMoveType',
 'PegOffsetType',
 'PegLimitType',
 'PegRoundDirection',
 'PeggedPrice',
 'PegScope',
 'DiscretionMoveType',
 'DiscretionOffsetType',
 'DiscretionLimitType',
 'DiscretionRoundDirection',
 'DiscretionPrice',
 'DiscretionScope',
 'TargetStrategy',
 'TargetStrategyParameters',
 'ParticipationRate',
 'TargetStrategyPerformance',
 'LastLiquidityInd',
 'PublishTrdIndicator',
 'ShortSaleReason',
 'QtyType',
 'SecondaryTrdType',
 'TradeReportType',
 'AllocNoOrdersType',
 'SharedCommission',
 'ConfirmReqID',
 'AvgParPx',
 'ReportedPx',
 'NoCapacities',
 'OrderCapacityQty',
 'NoEvents',
 'EventType',
 'EventDate',
 'EventPx',
 'EventText',
 'PctAtRisk',
 'NoInstrAttrib',
 'InstrAttribType',
 'InstrAttribValue',
 'DatedDate',
 'InterestAccrualDate',
 'CPProgram',
 'CPRegType',
 'UnderlyingCPProgram',
 'UnderlyingCPRegType',
 'UnderlyingQty',
 'TrdMatchID',
 'SecondaryTradeReportRefID',
 'UnderlyingDirtyPrice',
 'UnderlyingEndPrice',
 'UnderlyingStartValue',
 'UnderlyingCurrentValue',
 'UnderlyingEndValue',
 'NoUnderlyingStips',
 'UnderlyingStipType',
 'UnderlyingStipValue',
 'MaturityNetMoney',
 'MiscFeeBasis',
 'TotNoAllocs',
 'LastFragment',
 'CollReqID',
 'CollAsgnReason',
 'CollInquiryQualifier',
 'NoTrades',
 'MarginRatio',
 'MarginExcess',
 'TotalNetValue',
 'CashOutstanding',
 'CollAsgnID',
 'CollAsgnTransType',
 'CollRespID',
 'CollAsgnRespType',
 'CollAsgnRejectReason',
 'CollAsgnRefID',
 'CollRptID',
 'CollInquiryID',
 'CollStatus',
 'TotNumReports',
 'LastRptRequested',
 'AgreementDesc',
 'AgreementID',
 'AgreementDate',
 'StartDate',
 'EndDate',
 'AgreementCurrency',
 'DeliveryType',
 'EndAccruedInterestAmt',
 'StartCash',
 'EndCash',
 'UserRequestID',
 'UserRequestType',
 'NewPassword',
 'UserStatus',
 'UserStatusText',
 'StatusValue',
 'StatusText',
 'RefCompID',
 'RefSubID',
 'NetworkResponseID',
 'NetworkRequestID',
 'LastNetworkResponseID',
 'NetworkRequestType',
 'NoCompIDs',
 'NetworkStatusResponseType',
 'NoCollInquiryQualifier',
 'TrdRptStatus',
 'AffirmStatus',
 'UnderlyingStrikeCurrency',
 'LegStrikeCurrency',
 'TimeBracket',
 'CollAction',
 'CollInquiryStatus',
 'CollInquiryResult',
 'StrikeCurrency',
 'NoNested3PartyIDs',
 'Nested3PartyID',
 'Nested3PartyIDSource',
 'Nested3PartyRole',
 'NoNested3PartySubIDs',
 'Nested3PartySubID',
 'Nested3PartySubIDType',
 'LegContractSettlMonth',
 'LegInterestAccrualDate']

try:
    region, system, *spam = re.split('[\\\|/]', options.zip)
except TypeError:
    print('Param -p is empty')
    exit()
except ValueError:
    print('Not enouth params')
    exit()

try:
    os.mkdir(options.json)
except FileExistsError:
    pass
except TypeError:
    print('Param -o is empty')
    exit()

names = dict(zip(tags, values))


def read_file(directory):
    try:
        files = os.listdir(directory)
    except FileNotFoundError:
        files = []
        print('"{}" directory Not Found (search)'.format(directory))
        exit()
    if not files:
        yield directory
    for each_file in files:
        suf = ''.join(Path(each_file).suffixes)
        if suf == '.gz':
            open_func = gzip.open
        elif suf == '.bz2':
            open_func = bz2.BZ2File
        else:
            try:
                os.mkdir(res)
            except FileExistsError:
                pass
            open_func = open
        try:
            file_data = open_func('{}/{}'.format(directory, each_file), 'r')
        except FileNotFoundError:
            file_data = []
            print('File Not Found')
            exit()
        fix_row = file_data.read()
        if isinstance(fix_row, bytes):
            fix_row = fix_row.decode('utf-8')
        file_data.close()
        yield fix_row, '{}/{}'.format(res, each_file.split('.')[0])


def write_key_value(data):
    data = list(data)
    for each_data in data:
        if each_data[0]:
            if isinstance(each_data, str):
                print('"{}" directory is empty'.format(data[0]))
                break
            try:
                fix_split = each_data[0].replace('\x02', '').split('\x01')
                dict_data = dict(map(lambda items: (names.get(items.split('=')[0], items.split('=')[0]),
                                                    items.split('=')[1]),
                                     filter(None, fix_split)))
                new_dict_data = dict()
                new_dict_data['order_id'] = dict_data.get('OrderID', '')
                new_dict_data['trd_date'] = dict_data.get('TradeDate', '')
                new_dict_data['order_version'] = dict_data.get('10240', '')
                new_dict_data['region'] = region
                new_dict_data['system'] = system
                new_dict_data['message'] = ''.join(fix_split)
                with open('%s.json' % each_data[1], 'w') as json_data:
                    json.dump(
                        new_dict_data,
                        json_data
                    )
            except FileNotFoundError:
                print('File Not Found (record)')


write_key_value(read_file(options.zip))
