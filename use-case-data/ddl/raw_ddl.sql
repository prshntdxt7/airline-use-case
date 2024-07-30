-- note: we've chosen simple data types for our tables to focus on pipeline development and
-- data load operations.

CREATE TABLE `sunlit-analyst-430409-b3.raw.pax_data`
(
  Date STRING,
  Flight_Number STRING,
  Boarded_Y STRING,
  Capacity_Physical_Y STRING,
  Capacity_Saleable_Y STRING
);


CREATE TABLE `sunlit-analyst-430409-b3.raw.sales_data`
(
 Rank STRING,
 TransactionID STRING,
 Origin STRING,
 Destination STRING,
 Flight STRING,
 FlightDate STRING,
 ShortFlightDate STRING,
 FlightDateTime STRING,
 TransactionDate STRING,
 ShortTransactionDate STRING,
 TransactionDateTime STRING,
 AircraftRegistration STRING,
 AircraftType STRING,
 CrewMember STRING,
 ProductType STRING,
 ProductCode STRING,
 Quantity STRING,
 CostPrice STRING,
 NetSales STRING,
 VAT STRING,
 GrossSales STRING,
 TotalSales STRING,
 Cash STRING,
 Card STRING,
 Voucher STRING,
 PaymentDetails STRING,
 Reason STRING,
 Voided STRING,
 DeviceCode STRING,
 PaymentStatus STRING,
 BaseCurrencyPrice STRING,
 SaleItemID STRING,
 SaleItemStatus STRING,
 VatRateValue STRING,
 CrewBaseCode STRING,
 RefundReason STRING,
 PaymentId STRING,
 PriceType STRING,
 SeatNumber STRING,
 PassengerCount STRING,
 ScheduleActualDepartureTime STRING,
 ScheduleActualArrivalTime STRING,
 SaleTypeName STRING,
 PriceOverrideReason STRING,
 DiscountType STRING,
 HaulType STRING,
 SaleUploadDate STRING,
 SaleUploadTime STRING,
 Unnamed STRING
);



CREATE TABLE `sunlit-analyst-430409-b3.raw.loading_data`
(
  Flight_Month_Year STRING,
  Airline STRING,
  Flightnumber STRING,
  Departure_Unit STRING,
  Arrival_Unit STRING,
  Flight_Date STRING,
  Aircraft_Type STRING,
  Leg STRING,
  Booking_Class_CBASE STRING,
  Bill_of_material STRING,
  Price STRING,
  Invoice_Quantity STRING,
  Sales_and_Services_Invoice STRING,
  UAA_Invoice STRING
);