class MySQLQueries:

# ------------------------------------------------------------------------------------------   
# Contacts Query
# ------------------------------------------------------------------------------------------

    def __init__(self):
        self.ContactsQuery = """
        
        SELECT 
            vid AS 'Contact ID',
            CONCAT('=HYPERLINK("https://app.hubspot.com/contacts/2035494/contact/',vid,'","LINK")') AS 'Hubspot Link',
            lifecyclestage AS 'Lifecyclestage',
            market AS 'Market',
            sourced_by AS 'Sourced by',
            internal_original_source AS 'iOS',
            CONVERT( DATE(addedat) , CHAR) AS 'Createdate',
            CONVERT( DATE(lead_date_stamp) , CHAR) AS 'Lead Date',
            CONVERT( DATE(mql_date_stamp) , CHAR) AS 'MQL Date',
            CONVERT( DATE(sql_date_stamp) , CHAR) AS 'SQL Date'
        
        FROM
            companydata.contacts
        WHERE
            addedat >= '2020-01-01%'
            or lead_date_stamp >= '2020-01-01%'
            or mql_date_stamp >= '2020-01-01%'
            or sql_date_stamp >= '2020-01-01%'
            
            """

# ------------------------------------------------------------------------------------------   
# Deals Query
# ------------------------------------------------------------------------------------------

        self.DealsQuery = """

        SELECT
            dealid as "Deal ID",
            concat('=HYPERLINK("https://app.hubspot.com/contacts/2035494/deal/',dealid,'","LINK")') as "Hubspot Link",
            dealname as "Deal Name",
            amount_in_home_currency as "Deal Value",
            dealstage as "Dealstage",
            pipeline as "Pipeline",
            market as "Market",
            sourced_by as "Sourced by",
            sales_source as "Sales Source",
            internal_original_source as "iOS",
            case when opportunity is not null then convert(date(opportunity), CHAR) else convert(date(createdate), CHAR) end as "Opportunity Date",
            convert(date(business_case_proposal), CHAR) as "BCP Date",
            convert(date(negotiation), CHAR) as "Negotiation Date",
            convert(date(verbal_agreement), CHAR) as "VA Date",
            convert(date(closedate), CHAR) as "Closedate",
            convert(datediff(closedate, case when opportunity is not null then opportunity else createdate end), CHAR) as "Sales Cycle"

        FROM companydata.deals

        WHERE (case when opportunity is not null then opportunity >= "2020-01-01%" else createdate >= "2020-01-01%" end)
            or closedate >= "2020-01-01%"
            or dealstage in ("Business Case Proposal", "Negotiation (P, L & S)", "Opportunity", "Verbal Agreement")
            
                """
                
# ------------------------------------------------------------------------------------------   
# Pipeline Query
# ------------------------------------------------------------------------------------------

        self.PipelineQuery = """
        
            Select 
            convert(date(at_date), CHAR) as "Date", 
            case when sourced_by is null then "Unknown" else sourced_by end, 
            case when dealstage is null then "Unknown" else dealstage end, 
            case when market is null then "Unknown" else market end, 
            case when round(sum_amount_in_home_currency) is null then 0 else round(sum_amount_in_home_currency) end, 
            case when number_deals is null then 0 else number_deals end
            
            from deal_pipeline_history
                
        where at_date >= "2020-01-01%"
          and dealstage not in ("Closed Won - Enterprise", "Closed Lost")
            
        """

# ------------------------------------------------------------------------------------------   
# Funnel Query
# ------------------------------------------------------------------------------------------

        self.Funnel_Query = """

            SELECT 
                convert(date(Cal.date), CHAR) as "Date",
                case when A.Capterra_Cost is null then 0 else A.Capterra_Cost end, 
                case when B.FB_Cost is null then 0 else B.FB_Cost end, 
                case when C.Google_Cost is null then 0 else C.Google_Cost end, 
                case when D.LinkedIn_Cost is null then 0 else D.LinkedIn_Cost end, 
                case when E.Twitter_Cost is null then 0 else E.Twitter_Cost end, 
                case when F.Quora_Cost is null then 0 else F.Quora_Cost end, 
                case when G.Bing_Cost is null then 0 else G.Bing_Cost end 
                
                from (SELECT * FROM companydata.calendar
                    where Year(date) > 2019
                    and year(date) < year(now())+1) as Cal
                    
                    left join (SELECT Date, sum(cost) as "Capterra_Cost" FROM companydata.funnel_data_capterra
                        group by year(date), month(date), day(date)) A
                    on Cal.Date = A.Date
                    
                    left join (SELECT Date, sum(cost) as "FB_Cost" FROM companydata.funnel_data_facebook_ads
                        group by year(date), month(date), day(date)) B
                    on Cal.Date = B.Date
                    
                    left join (SELECT Date, sum(cost) as "Google_Cost" FROM companydata.funnel_data_google_ads
                        group by year(date), month(date), day(date)) C
                    on Cal.Date = C.Date
                    
                    left join (SELECT Date, sum(cost) as "LinkedIn_Cost" FROM companydata.funnel_data_linkedin
                        group by year(date), month(date), day(date)) D
                    on Cal.Date = D.Date
                    
                    left join (SELECT Date, sum(cost) as "Twitter_Cost" FROM companydata.funnel_data_twitter
                        group by year(date), month(date), day(date)) E
                    on Cal.Date = E.Date
                    
                    left join (SELECT Date, sum(cost) as "Quora_Cost" FROM companydata.funnel_data_quora
                        group by year(date), month(date), day(date)) F
                    on Cal.Date = F.Date
                    
                    left join (SELECT Date, sum(cost) as "Bing_Cost" FROM companydata.funnel_data_microsoft_ads
                        group by year(date), month(date), day(date)) G
                    on Cal.Date = G.Date
                    
             order by date

        """

# ------------------------------------------------------------------------------------------   
# Returning Output
# ------------------------------------------------------------------------------------------

    def get_ContactsQuery(self):
        return self.ContactsQuery

    def get_DealsQuery(self):
        return self.DealsQuery

    def get_PipelineQuery(self):
        return self.PipelineQuery

    def get_FunnelQuery(self):
        return self.Funnel_Query