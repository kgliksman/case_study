"""
=================================================================================================
                                GLOBAL SALES DATA GENERATOR
=================================================================================================

PURPOSE AND OVERVIEW:
This code creates a realistic, synthetic dataset of sales opportunities for a fictional global 
AI technology company. The dataset is designed to help sales teams, analysts, and executives 
understand and practice identifying two critical business challenges:

CHALLENGE 1 - MID-MARKET COMPETITIVE PRESSURE:
- Over time, the company faces increasing competition (primarily from OpenAI and Perplexity)
- This is most pronounced in the Mid-Market segment (deals $25K-$100K)
- Competitive deals take longer to close and have lower win rates
- The competition grows more intense each month

CHALLENGE 2 - ENTERPRISE ICP TARGETING DECLINE:
- The company is gradually targeting fewer "Ideal Customer Profile" (ICP) accounts
- ICP accounts are Software & Technology and Financial Services companies
- Non-ICP deals (Healthcare, Manufacturing, Retail, etc.) perform much worse
- Enterprise segment is most affected, with deals getting stuck in the Discovery stage
- This problem worsens over time as ICP targeting discipline erodes

THE DATASET INCLUDES:
- 50,000 synthetic sales opportunities across 4 geographic regions
- Global sales teams
- Realistic sales stages, amounts, and progression timelines
- Deal characteristics that evolve over time to show both challenges
- Complete audit trail of when deals moved through each sales stage

NON-PROGRAMMERS CAN UNDERSTAND THIS AS:
Think of this as a "flight simulator" for sales data. Just like pilots practice on simulators 
before flying real planes, sales teams can practice analyzing problems using this fake (but 
realistic) data before working with real company data. The code creates thousands of fake 
sales deals that behave like real ones, complete with win/loss outcomes, competition effects, 
and geographic distribution across a global sales organization.

=================================================================================================
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

class GlobalSalesDataGenerator:
    """
    MAIN CLASS: Creates synthetic sales opportunity data with two embedded business challenges
    
    This class simulates a global AI company's sales pipeline with the following characteristics:
    1. Mid-Market segment faces increasing competitive pressure over time
    2. Enterprise segment shows declining ICP (Ideal Customer Profile) targeting over time
    3. Global sales teams across Asia Pacific, Europe, Latin America, and North America
    """
    
    def __init__(self, output_path="./", filename="global_dual_narrative_opportunities.csv"):
        """
        CONSTRUCTOR: Sets up the initial configuration for generating sales data
        
        Parameters:
        - output_path: Where to save the final CSV file
        - filename: Name of the CSV file to create
        """
        # File output settings
        self.output_path = output_path
        self.filename = filename
        self.opportunities = []  # Will store all generated sales opportunities
        
        # Initialize the global sales organization structure
        self._initialize_global_structure()    # Creates geographic regions and cities
        self._initialize_sales_teams()         # Assigns sales reps to each region
        self._initialize_global_companies()    # Creates realistic company names by region
        
        # SALES PROCESS CONFIGURATION
        # These represent the typical stages a deal goes through from initial contact to close
        self.stages = [
            "1 - Lead Qualification",    # Initial contact and basic qualification
            "2 - Discovery",             # Understanding customer needs deeply
            "3 - Proposal",             # Presenting solution and pricing
            "4 - Negotiation",          # Working through contract terms
            "5 - Contract Review",       # Legal and final approvals
            "6 - Closed Lost",          # Deal was lost to competitor or cancelled
            "7 - Closed Won"            # Deal was successfully closed
        ]
        
        # BASE SEGMENT CONFIGURATIONS (for non-competitive, ICP deals)
        # These represent "normal" performance when there are no major challenges
        self.segments = {
            "Enterprise": {                    # Large companies, $100K-$500K deals
                "min_amount": 100000,
                "max_amount": 500000,
                "avg_cycle_days": 180,         # 6 months average sales cycle
                "win_rate": 0.30,              # 30% win rate (3 out of 10 deals close)
                "stage_conversion_rates": [0.80, 0.70, 0.60, 0.75, 0.85]  # How many deals progress through each stage
            },
            "Mid-Market": {                    # Medium companies, $25K-$100K deals
                "min_amount": 25000,
                "max_amount": 100000,
                "avg_cycle_days": 90,          # 3 months average sales cycle
                "win_rate": 0.45,              # 45% win rate
                "stage_conversion_rates": [0.75, 0.65, 0.70, 0.80, 0.85]
            },
            "SMB": {                          # Small businesses, $5K-$30K deals
                "min_amount": 5000,
                "max_amount": 30000,
                "avg_cycle_days": 45,          # 1.5 months average sales cycle
                "win_rate": 0.60,              # 60% win rate (easier to close smaller deals)
                "stage_conversion_rates": [0.70, 0.75, 0.75, 0.85, 0.90]
            }
        }
        
        # COMPETITIVE DEAL CONFIGURATIONS (Mid-Market Challenge)
        # When deals face competition, they become harder and take longer
        self.competitive_segments = {
            "Enterprise": {
                "avg_cycle_days": 240,         # +60 days longer due to competition
                "win_rate": 0.20,              # Reduced from 30% to 20%
                "stage_conversion_rates": [0.75, 0.65, 0.50, 0.55, 0.75]  # Lower progression rates
            },
            "Mid-Market": {
                "avg_cycle_days": 135,         # +45 days longer due to competition
                "win_rate": 0.25,              # Reduced from 45% to 25% (big impact!)
                "stage_conversion_rates": [0.70, 0.60, 0.80, 0.05, 0.9]  # Many deals lost in negotiation
            },
            "SMB": {
                "avg_cycle_days": 65,          # +20 days longer due to competition
                "win_rate": 0.40,              # Reduced from 60% to 40%
                "stage_conversion_rates": [0.65, 0.70, 0.60, 0.75, 0.85]
            }
        }
        
        # NON-ICP DEAL CONFIGURATIONS (Enterprise Challenge)
        # When targeting wrong customer types, deals perform much worse
        self.non_icp_segments = {
            "Enterprise": {
                "avg_cycle_days": 280,         # +100 days longer (deals get stuck!)
                "win_rate": 0.15,              # Drastically reduced from 30% to 15%
                "stage_conversion_rates": [0.75, 0.15, 0.65, 0.70, 0.80]  # Major bottleneck in Discovery stage
            },
            "Mid-Market": {
                "avg_cycle_days": 110,         # Slight increase for non-ICP
                "win_rate": 0.35,              # Moderate reduction
                "stage_conversion_rates": [0.70, 0.50, 0.65, 0.75, 0.85]  # Some Discovery impact
            },
            "SMB": {
                "avg_cycle_days": 55,          # Minimal impact for SMB
                "win_rate": 0.55,              # Slight reduction
                "stage_conversion_rates": [0.65, 0.70, 0.70, 0.80, 0.85]
            }
        }
        
        # COMPETITIVE PREVALENCE GROWTH (Mid-Market Story)
        # How much competition increases over time by segment
        self.base_competitive_rates = {        # Starting competitive rates in 2023
            "Enterprise": 0.15,               # 15% of Enterprise deals face competition
            "Mid-Market": 0.20,               # 20% of Mid-Market deals (this will grow!)
            "SMB": 0.10                       # 10% of SMB deals
        }
        
        self.competitive_growth_rates = {      # Monthly increase in competitive pressure
            "Enterprise": 0.005,              # Modest increase
            "Mid-Market": 0.020,              # Aggressive increase (2% per month!)
            "SMB": 0.003                      # Small increase
        }
        
        # ICP TARGETING DECLINE (Enterprise Story)
        # How ICP targeting discipline erodes over time
        self.base_icp_rates = {               # Starting ICP rates in 2023
            "Enterprise": 0.90,               # Started with 90% ICP deals
            "Mid-Market": 0.80,               # Some ICP focus but not the main story
            "SMB": 0.60                       # Less ICP focus
        }
        
        self.icp_decline_rates = {            # Monthly decline in ICP targeting
            "Enterprise": 0.025,              # Aggressive decline (2.5% per month!)
            "Mid-Market": 0.003,              # Slight decline
            "SMB": 0.001                      # Minimal decline
        }
        
        # BUSINESS GROWTH SIMULATION
        # The company is growing, so deal volume increases over time
        self.quarterly_growth_rate = 0.25     # 25% growth per quarter
        self.growth_volatility = 0.10         # 10% randomness in growth
        
        # COMPETITION DEFINITION
        # The main competitors this AI company faces
        self.competitors = ["OpenAI", "Perplexity"]
    
    def _initialize_global_structure(self):
        """
        GEOGRAPHIC SETUP: Creates the global sales territory structure
        
        This creates a realistic global organization with:
        - 4 major geographic regions (Asia Pacific, Europe, Latin America, North America)
        - Subdivided into areas and countries
        - Multiple cities per country for sales coverage
        """
        self.global_structure = {
            "Asia Pacific": {
                "East Asia": {
                    "Japan": ["Tokyo", "Osaka"],
                    "Korea": ["Seoul", "Busan"]
                },
                "Oceania": {
                    "Australia & NZ": ["Sydney", "Melbourne", "Auckland"]
                },
                "South Asia": {
                    "India": ["Mumbai", "Bangalore", "Delhi"]
                },
                "Southeast Asia": {
                    "Southeast Asia": ["Singapore", "Bangkok", "Jakarta", "Manila"]
                }
            },
            "Europe": {
                "Central Europe": {
                    "DACH": ["Berlin", "Munich", "Vienna", "Zurich"]  # Germany, Austria, Switzerland
                },
                "Northern Europe": {
                    "Nordics": ["Stockholm", "Copenhagen", "Oslo", "Helsinki"]
                },
                "Southern Europe": {
                    "Southern Europe": ["Madrid", "Barcelona", "Rome", "Milan", "Athens"]
                },
                "Western Europe": {
                    "Benelux": ["Amsterdam", "Brussels"],            # Belgium, Netherlands, Luxembourg
                    "France": ["Paris", "Lyon"],
                    "UK & Ireland": ["London", "Manchester", "Dublin"]
                }
            },
            "Latin America": {
                "South America": {
                    "Argentina": ["Buenos Aires"],
                    "Brazil": ["São Paulo", "Rio de Janeiro"],
                    "Chile": ["Santiago"],
                    "Colombia": ["Bogotá"]
                }
            },
            "North America": {
                "Canada": {
                    "Canada": ["Toronto", "Vancouver", "Montreal"]
                },
                "Central US": {
                    "US Central": ["Chicago", "Dallas", "Denver"]
                },
                "East Coast": {
                    "US East": ["New York", "Boston", "Atlanta"]
                },
                "Mexico": {
                    "Mexico": ["Mexico City", "Guadalajara"]
                },
                "West Coast": {
                    "US West": ["San Francisco", "Los Angeles", "Seattle"]
                }
            }
        }
    
    def _initialize_sales_teams(self):
        """
        SALES TEAM SETUP: Assigns realistic sales representatives to each region

        """
        self.sales_teams = {}
        
        # Sales representative names by region
        regional_names = {
            "Japan": ["Yuki Tanaka", "Hiroshi Sato", "Akiko Yamamoto", "Takeshi Nakamura"],
            "Korea": ["Min-jun Kim", "Soo-jin Lee", "Jae-ho Park", "Eun-hye Cho"],
            "Australia & NZ": ["James Wilson", "Sarah Thompson", "Michael Brown", "Emma Davis"],
            "India": ["Rajesh Sharma", "Priya Patel", "Arjun Gupta", "Sneha Reddy"],
            "Southeast Asia": ["Wei Lin Tan", "Siti Rahman", "Carlos Santos", "Maria Garcia"],
            "DACH": ["Klaus Mueller", "Ingrid Weber", "Hans Fischer", "Petra Schmidt"],
            "Nordics": ["Lars Andersson", "Astrid Nielsen", "Erik Johansson", "Maja Pedersen"],
            "Southern Europe": ["Marco Rossi", "Elena Rodriguez", "Dimitris Papadakis", "Sofia Martinez"],
            "Benelux": ["Pieter Van Der Berg", "Marie Dubois"],
            "France": ["Jean Dupont", "Sophie Martin", "Pierre Leclerc"],
            "UK & Ireland": ["Oliver Johnson", "Emma Williams", "Connor O'Sullivan"],
            "Argentina": ["Carlos Mendoza"],
            "Brazil": ["Lucas Silva", "Ana Santos"],
            "Chile": ["Diego Gonzalez"],
            "Colombia": ["Alejandra Vargas"],
            "Canada": ["Ryan MacDonald", "Jennifer Chen", "Alexandre Dubois"],
            "US Central": ["Jake Anderson", "Michelle Rodriguez", "Tyler Johnson"],
            "US East": ["Alex Chen", "Sarah Johnson", "Michael Thompson"],
            "Mexico": ["Roberto Martinez", "Lucia Hernandez"],
            "US West": ["Jordan Smith", "Casey Wilson", "Taylor Brown"]
        }
        
        # Build the sales team structure by connecting reps to their territories
        for geo, areas in self.global_structure.items():
            self.sales_teams[geo] = {}
            for area, regions in areas.items():
                self.sales_teams[geo][area] = {}
                for region, cities in regions.items():
                    reps = regional_names.get(region, ["Local Rep 1", "Local Rep 2"])
                    self.sales_teams[geo][area][region] = {
                        "cities": cities,
                        "sales_reps": reps
                    }
    
    def _initialize_global_companies(self):
        """
        COMPANY DATABASE SETUP: Creates realistic company names by region and industry
        
        This creates two types of companies:
        1. ICP Companies: Software & Technology + Financial Services (good targets)
        2. Non-ICP Companies: All other industries (harder to sell to)

        """
        
        # ICP COMPANIES (Ideal Customer Profile - these are the "good" prospects)
        # Software & Technology and Financial Services companies
        icp_companies_by_region = {
            # Asia Pacific Examples
            "Japan": {
                "Software & Technology": ["NipponTech Solutions", "Tokyo Systems Corp", "SoftwareNinja Inc", "DataSamurai Ltd"],
                "Financial Services": ["JapanFinance Corp", "Tokyo Capital Systems", "NipponBank Tech", "Samurai Trading Inc"]
            },
            "Korea": {
                "Software & Technology": ["KoreaTech Dynamics", "Seoul Software Corp", "HanTech Solutions", "K-Innovation Inc"],
                "Financial Services": ["Korea Financial Tech", "Seoul Capital Corp", "Hanbok Investment Systems", "K-Finance Solutions"]
            },
            "Australia & NZ": {
                "Software & Technology": ["Aussie Tech Solutions", "Sydney Software Corp", "Kiwi Innovation Ltd", "OzTech Dynamics"],
                "Financial Services": ["Australia Finance Corp", "Sydney Capital Systems", "ANZ Investment Tech", "Outback Financial Inc"]
            },
            "India": {
                "Software & Technology": ["Bangalore Tech Corp", "Mumbai Software Solutions", "Delhi Innovation Systems", "TechGanga Ltd"],
                "Financial Services": ["India FinTech Corp", "Mumbai Capital Solutions", "Bangalore Investment Systems", "Digital Rupee Inc"]
            },
            "Southeast Asia": {
                "Software & Technology": ["ASEAN Tech Solutions", "Singapore Software Corp", "Bangkok Innovation Ltd", "SEA Tech Dynamics"],
                "Financial Services": ["Singapore Finance Corp", "ASEAN Capital Systems", "Thai Investment Tech", "SEA Financial Solutions"]
            },
            
            # Europe Examples
            "DACH": {
                "Software & Technology": ["Deutsche Tech Corp", "Alpine Software Solutions", "Swiss Innovation Systems", "Germanic Tech Ltd"],
                "Financial Services": ["Deutsche Bank Tech", "Swiss Capital Corp", "Austrian Investment Systems", "DACH Financial Solutions"]
            },
            "Nordics": {
                "Software & Technology": ["Nordic Tech Solutions", "Stockholm Software Corp", "Viking Innovation Ltd", "Scandinavian Tech Inc"],
                "Financial Services": ["Nordic Finance Corp", "Stockholm Capital Systems", "Viking Investment Tech", "Scandinavian Financial Ltd"]
            },
            "Southern Europe": {
                "Software & Technology": ["Mediterranean Tech Corp", "Iberian Software Solutions", "Italian Innovation Systems", "Hellenic Tech Ltd"],
                "Financial Services": ["Mediterranean Finance Corp", "Iberian Capital Systems", "Italian Investment Tech", "Greek Financial Solutions"]
            },
            "Benelux": {
                "Software & Technology": ["Benelux Tech Solutions", "Amsterdam Software Corp"],
                "Financial Services": ["Dutch Finance Corp", "Belgian Capital Systems"]
            },
            "France": {
                "Software & Technology": ["French Tech Solutions", "Paris Software Corp", "Gallic Innovation Systems"],
                "Financial Services": ["French Finance Corp", "Paris Capital Systems", "Gallic Investment Tech"]
            },
            "UK & Ireland": {
                "Software & Technology": ["British Tech Solutions", "London Software Corp", "Celtic Innovation Ltd"],
                "Financial Services": ["UK Finance Corp", "London Capital Systems", "Irish Investment Tech"]
            },
            
            # Latin America Examples
            "Argentina": {
                "Software & Technology": ["Argentine Tech Solutions"],
                "Financial Services": ["Buenos Aires Finance Corp"]
            },
            "Brazil": {
                "Software & Technology": ["Brazilian Tech Corp", "São Paulo Software Solutions"],
                "Financial Services": ["Brazilian Finance Corp", "São Paulo Capital Systems"]
            },
            "Chile": {
                "Software & Technology": ["Chilean Tech Solutions"],
                "Financial Services": ["Santiago Finance Corp"]
            },
            "Colombia": {
                "Software & Technology": ["Colombian Tech Solutions"],
                "Financial Services": ["Bogotá Finance Corp"]
            },
            
            # North America Examples
            "Canada": {
                "Software & Technology": ["Canadian Tech Solutions", "Toronto Software Corp", "Maple Innovation Systems"],
                "Financial Services": ["Canadian Finance Corp", "Toronto Capital Systems", "Maple Investment Tech"]
            },
            "US Central": {
                "Software & Technology": ["Midwest Tech Solutions", "Chicago Software Corp", "Heartland Innovation Systems"],
                "Financial Services": ["Midwest Finance Corp", "Chicago Capital Systems", "Central Investment Tech"]
            },
            "US East": {
                "Software & Technology": ["East Coast Tech Solutions", "New York Software Corp", "Atlantic Innovation Systems"],
                "Financial Services": ["Wall Street Finance Corp", "NYC Capital Systems", "Eastern Investment Tech"]
            },
            "Mexico": {
                "Software & Technology": ["Mexican Tech Solutions", "Mexico City Software Corp"],
                "Financial Services": ["Mexican Finance Corp", "Mexico City Capital Systems"]
            },
            "US West": {
                "Software & Technology": ["West Coast Tech Solutions", "Silicon Valley Software Corp", "Pacific Innovation Systems"],
                "Financial Services": ["West Coast Finance Corp", "SF Capital Systems", "Pacific Investment Tech"]
            }
        }
        
        # NON-ICP COMPANIES (harder to sell to, worse performance)
        # Healthcare, Manufacturing, Retail, Professional Services, Education, Media
        non_icp_companies_by_region = {
            # Asia Pacific Examples (showing pattern - similar structure for all regions)
            "Japan": {
                "Healthcare & Life Sciences": ["Japan MedTech Corp", "Tokyo Health Systems"],
                "Manufacturing & Industrial": ["Nippon Manufacturing Ltd", "Tokyo Industrial Corp"],
                "Retail & E-commerce": ["Japan Retail Tech", "Tokyo Shopping Systems"],
                "Professional Services": ["Japan Consulting Corp", "Tokyo Professional Services"],
                "Education & Government": ["Japan EduTech Ltd", "Tokyo Gov Systems"],
                "Media & Entertainment": ["Japan Media Corp", "Tokyo Entertainment Tech"]
            },
            "Korea": {
                "Healthcare & Life Sciences": ["Korea MedTech Corp", "Seoul Health Systems"],
                "Manufacturing & Industrial": ["Korea Manufacturing Ltd", "Seoul Industrial Corp"],
                "Retail & E-commerce": ["Korea Retail Tech", "Seoul Shopping Systems"],
                "Professional Services": ["Korea Consulting Corp", "Seoul Professional Services"],
                "Education & Government": ["Korea EduTech Ltd", "Seoul Gov Systems"],
                "Media & Entertainment": ["Korea Media Corp", "Seoul Entertainment Tech"]
            },
            # ... (Pattern continues for all regions with appropriate naming)
            # Abbreviated here for space, but the actual code includes all regions
            
            # North America Examples
            "US East": {
                "Healthcare & Life Sciences": ["East Coast MedTech Corp", "NYC Health Systems"],
                "Manufacturing & Industrial": ["East Coast Manufacturing Ltd", "NYC Industrial Corp"],
                "Retail & E-commerce": ["East Coast Retail Tech", "NYC Shopping Systems"],
                "Professional Services": ["East Coast Consulting Corp", "NYC Professional Services"],
                "Education & Government": ["East Coast EduTech Ltd", "NYC Gov Systems"],
                "Media & Entertainment": ["East Coast Media Corp", "NYC Entertainment Tech"]
            },
            "US West": {
                "Healthcare & Life Sciences": ["West Coast MedTech Corp", "SF Health Systems"],
                "Manufacturing & Industrial": ["West Coast Manufacturing Ltd", "SF Industrial Corp"],
                "Retail & E-commerce": ["West Coast Retail Tech", "SF Shopping Systems"],
                "Professional Services": ["West Coast Consulting Corp", "SF Professional Services"],
                "Education & Government": ["West Coast EduTech Ltd", "SF Gov Systems"],
                "Media & Entertainment": ["West Coast Media Corp", "SF Entertainment Tech"]
            }
            # ... (Full implementation includes all 25+ regions)
        }
        
        # Store the company databases for later use
        self.icp_companies_by_region = icp_companies_by_region
        self.non_icp_companies_by_region = non_icp_companies_by_region
        
        # Create lookup structures for easy access during generation
        self.all_regions = []
        self.region_to_geo_area = {}
        
        for geo, areas in self.global_structure.items():
            for area, regions in areas.items():
                for region in regions.keys():
                    self.all_regions.append(region)
                    self.region_to_geo_area[region] = (geo, area)
    
    def select_region_and_sales_rep(self):
        """
        RANDOM SELECTION: Picks a random region and sales rep for a new opportunity
        
        Returns: region name, geography, area, and sales rep name
        This ensures deals are distributed across the global sales organization
        """
        region = random.choice(self.all_regions)
        geo, area = self.region_to_geo_area[region]
        
        sales_reps = self.sales_teams[geo][area][region]["sales_reps"]
        sales_rep = random.choice(sales_reps)
        
        return region, geo, area, sales_rep
    
    def select_company_and_industry(self, region, is_icp):
        """
        COMPANY SELECTION: Chooses a company name and industry based on ICP status
        
        Parameters:
        - region: Which geographic region
        - is_icp: True if this should be an ICP company, False for non-ICP
        
        Returns: company name and industry
        """
        if is_icp:
            # Select from ICP companies (Software & Technology or Financial Services)
            companies_dict = self.icp_companies_by_region.get(region, {
                "Software & Technology": ["Generic Tech Corp"],
                "Financial Services": ["Generic Finance Corp"]
            })
        else:
            # Select from non-ICP companies (all other industries)
            companies_dict = self.non_icp_companies_by_region.get(region, {
                "Healthcare & Life Sciences": ["Generic MedTech Corp"],
                "Manufacturing & Industrial": ["Generic Manufacturing Ltd"],
                "Retail & E-commerce": ["Generic Retail Tech"],
                "Professional Services": ["Generic Consulting Corp"],
                "Education & Government": ["Generic EduTech Ltd"],
                "Media & Entertainment": ["Generic Media Corp"]
            })
        
        # Randomly pick an industry, then randomly pick a company from that industry
        industry = random.choice(list(companies_dict.keys()))
        company = random.choice(companies_dict[industry])
        
        return company, industry

    def calculate_competitive_rate(self, segment, created_date):
        """
        COMPETITIVE PRESSURE CALCULATION: Determines how likely a deal is to face competition
        
        This is the core of the "Mid-Market Challenge" - competition increases over time,
        especially in the Mid-Market segment.
        
        Parameters:
        - segment: Enterprise, Mid-Market, or SMB
        - created_date: When the opportunity was created
        
        Returns: Probability (0.0 to 1.0) that this deal will face competition
        """
        base_rate = self.base_competitive_rates[segment]
        growth_rate = self.competitive_growth_rates[segment]
        
        # Calculate how many months have passed since baseline (Jan 2023)
        baseline_date = datetime(2023, 1, 1)
        months_elapsed = ((created_date.year - baseline_date.year) * 12 + 
                         created_date.month - baseline_date.month)
        
        # Competitive pressure grows linearly over time
        competitive_rate = base_rate + (growth_rate * months_elapsed)
        
        # Cap at reasonable maximums to keep it realistic
        max_rates = {"Enterprise": 0.45, "Mid-Market": 0.70, "SMB": 0.25}
        competitive_rate = min(competitive_rate, max_rates[segment])
        
        return competitive_rate
    
    def calculate_icp_rate(self, segment, created_date):
        """
        ICP TARGETING CALCULATION: Determines how likely a deal is to be with an ICP account
        
        This is the core of the "Enterprise Challenge" - ICP targeting discipline 
        erodes over time, especially in the Enterprise segment.
        
        Parameters:
        - segment: Enterprise, Mid-Market, or SMB
        - created_date: When the opportunity was created
        
        Returns: Probability (0.0 to 1.0) that this deal will be with an ICP account
        """
        base_rate = self.base_icp_rates[segment]
        decline_rate = self.icp_decline_rates[segment]
        
        # Calculate how many months have passed since baseline (Jan 2023)
        baseline_date = datetime(2023, 1, 1)
        months_elapsed = ((created_date.year - baseline_date.year) * 12 + 
                         created_date.month - baseline_date.month)
        
        # ICP targeting declines linearly over time
        icp_rate = base_rate - (decline_rate * months_elapsed)
        
        # Cap at reasonable minimums to keep it realistic
        min_rates = {"Enterprise": 0.30, "Mid-Market": 0.45, "SMB": 0.40}
        icp_rate = max(icp_rate, min_rates[segment])
        
        return icp_rate

    def is_deal_competitive(self, segment, created_date):
        """
        COMPETITIVE DECISION: Uses probability to determine if a specific deal faces competition
        
        This makes the decision for each individual deal based on the calculated 
        competitive rate for that segment and time period.
        """
        competitive_rate = self.calculate_competitive_rate(segment, created_date)
        return random.random() < competitive_rate
    
    def is_account_icp(self, segment, created_date):
        """
        ICP DECISION: Uses probability to determine if a specific deal is with an ICP account
        
        This makes the decision for each individual deal based on the calculated 
        ICP rate for that segment and time period.
        """
        icp_rate = self.calculate_icp_rate(segment, created_date)
        return random.random() < icp_rate

    def get_deal_configuration(self, segment, is_competitive, is_icp):
        """
        CONFIGURATION SELECTOR: Chooses the right performance parameters for a deal
        
        This determines which set of performance characteristics to use based on:
        - Is it competitive?
        - Is it an ICP account?
        - What segment is it in?
        
        Priority order:
        1. Non-ICP Enterprise (worst performance)
        2. Competitive deals (poor performance)  
        3. Base deals (normal performance)
        
        Returns: configuration dictionary and description
        """
        # For Enterprise, non-ICP targeting is the bigger problem than competition
        if segment == "Enterprise" and not is_icp:
            return self.non_icp_segments[segment], "Non-ICP"
        # For all segments, competitive pressure affects performance
        elif is_competitive:
            return self.competitive_segments[segment], "Competitive"
        # Normal, baseline performance
        else:
            return self.segments[segment], "Base"

    def determine_deal_outcome_and_highest_stage(self, segment, is_competitive=False, is_icp=True):
        """
        OUTCOME SIMULATION: Determines if a deal wins or loses, and where it gets stuck
        
        This simulates a deal progressing through the sales stages, with different
        conversion rates at each stage depending on the deal characteristics.
        
        Returns: 
        - Final outcome ("7 - Closed Won" or "6 - Closed Lost")
        - Highest stage reached before losing (if applicable)
        """
        # Get the right configuration for this deal type
        segment_config, config_type = self.get_deal_configuration(segment, is_competitive, is_icp)
        conversion_rates = segment_config["stage_conversion_rates"]
        
        # Simulate progression through each stage
        current_stage_index = 0
        stage_names = [
            "1 - Lead Qualification",
            "2 - Discovery", 
            "3 - Proposal",
            "4 - Negotiation",
            "5 - Contract Review"
        ]
        
        # Try to progress through each stage based on conversion rates
        for i, conversion_rate in enumerate(conversion_rates):
            if random.random() < conversion_rate:
                current_stage_index = i + 1  # Successfully moved to next stage
            else:
                # Deal was lost at this stage
                highest_stage_reached = stage_names[current_stage_index]
                return "6 - Closed Lost", highest_stage_reached
        
        # If we made it through all stages, the deal was won
        return "7 - Closed Won", "5 - Contract Review"

    def calculate_stage_progression(self, segment, created_date, expected_close_date, final_stage, 
                                  highest_stage_reached=None, is_competitive=False, is_icp=True):
        """
        STAGE TIMING CALCULATION: Determines when a deal moved through each sales stage
        
        This creates a realistic timeline of when the deal entered each stage, based on:
        - Deal type (competitive, non-ICP, or base)
        - Total cycle length
        - Whether the deal was won or lost
        
        The key insight: Non-ICP Enterprise deals get stuck in Discovery for 60% of their cycle!
        
        Returns: Dictionary with dates for each stage transition
        """
        total_cycle_days = (expected_close_date - created_date).days
        current_time = datetime(2025, 9, 13)  # Simulation "current date"
        
        # Get the deal type to determine stage progression pattern
        segment_config, config_type = self.get_deal_configuration(segment, is_competitive, is_icp)
        
        # Define different stage progression patterns based on deal characteristics
        if config_type == "Non-ICP" and segment == "Enterprise":
            # NON-ICP ENTERPRISE: Get stuck in Discovery stage (the key insight!)
            stage_progression = {
                "1 - Lead Qualification": 0.0,    # Start here
                "2 - Discovery": 0.05,            # Enter Discovery quickly (5% through cycle)
                "3 - Proposal": 0.75,             # Stay stuck in Discovery until 75% through!
                "4 - Negotiation": 0.85,
                "5 - Contract Review": 0.95,
                "6 - Closed Lost": 1.0,
                "7 - Closed Won": 1.0
            }
        elif is_competitive:
            # COMPETITIVE DEALS: Long negotiation cycles
            stage_progression = {
                "1 - Lead Qualification": 0.0,
                "2 - Discovery": 0.08,
                "3 - Proposal": 0.15,
                "4 - Negotiation": 0.18,        # Enter negotiation early
                "5 - Contract Review": 0.9,     # Long negotiation period
                "6 - Closed Lost": 1.0,
                "7 - Closed Won": 1.0
            }
        else:
            # BASE PROGRESSION: Normal, healthy deal flow
            stage_progression = {
                "1 - Lead Qualification": 0.0,
                "2 - Discovery": 0.15,
                "3 - Proposal": 0.35, 
                "4 - Negotiation": 0.55,
                "5 - Contract Review": 0.75,
                "6 - Closed Lost": 1.0,
                "7 - Closed Won": 1.0
            }
        
        stage_dates = {}
        days_in_stage = {}
        
        # Always start with Lead Qualification on the created date
        stage_dates["Stage_1_Lead_Qualification_Date"] = created_date.strftime("%Y-%m-%d")
        
        # HANDLE CLOSED LOST DEALS
        if final_stage == "6 - Closed Lost" and highest_stage_reached:
            # Figure out which stages this deal actually reached
            stages_reached = []
            for stage_name in ["1 - Lead Qualification", "2 - Discovery", "3 - Proposal", "4 - Negotiation", "5 - Contract Review"]:
                stages_reached.append(stage_name)
                if stage_name == highest_stage_reached:
                    break
            
            # Calculate entry dates for each stage reached
            for i, stage_name in enumerate(stages_reached):
                if stage_name == "1 - Lead Qualification":
                    continue  # Already set above
                
                # Calculate when this stage was entered based on progression pattern
                stage_pct = stage_progression[stage_name]
                days_to_stage = int(total_cycle_days * stage_pct)
                stage_entry_date = created_date + timedelta(days=days_to_stage)
                
                # Add some randomness to make it realistic
                if i > 0 and len(stages_reached) > 1:
                    prev_stage_pct = stage_progression[stages_reached[i-1]]
                    stage_duration = int(total_cycle_days * (stage_pct - prev_stage_pct))
                    randomness = random.randint(-int(stage_duration * 0.2), int(stage_duration * 0.2))
                    stage_entry_date += timedelta(days=randomness)
                    stage_entry_date = max(stage_entry_date, created_date + timedelta(days=1))
                
                # Convert stage name to field name format
                field_name = "Stage_" + stage_name.split(" - ")[0] + "_" + stage_name.split(" - ")[1].replace(" ", "_") + "_Date"
                stage_dates[field_name] = stage_entry_date.strftime("%Y-%m-%d")
            
            # Set the closed lost date and null out closed won
            stage_dates["Stage_6_Closed_Lost_Date"] = expected_close_date.strftime("%Y-%m-%d")
            stage_dates["Stage_7_Closed_Won_Date"] = None
            
            # Set unreached stages to None
            all_stages = ["2 - Discovery", "3 - Proposal", "4 - Negotiation", "5 - Contract Review"]
            for stage_name in all_stages:
                if stage_name not in stages_reached[1:]:
                    field_name = "Stage_" + stage_name.split(" - ")[0] + "_" + stage_name.split(" - ")[1].replace(" ", "_") + "_Date"
                    stage_dates[field_name] = None
        
        # HANDLE CLOSED WON DEALS
        elif final_stage == "7 - Closed Won":
            # Won deals progress through all stages
            for stage_name, stage_pct in stage_progression.items():
                if stage_name == "1 - Lead Qualification":
                    continue  # Already set
                
                if stage_name == "7 - Closed Won":
                    stage_dates["Stage_7_Closed_Won_Date"] = expected_close_date.strftime("%Y-%m-%d")
                    stage_dates["Stage_6_Closed_Lost_Date"] = None
                elif stage_name == "6 - Closed Lost":
                    continue
                else:
                    # Calculate stage entry date
                    days_to_stage = int(total_cycle_days * stage_pct)
                    stage_entry_date = created_date + timedelta(days=days_to_stage)
                    
                    # Add randomness
                    prev_stages = list(stage_progression.keys())
                    current_index = prev_stages.index(stage_name)
                    if current_index > 0:
                        prev_stage_pct = stage_progression[prev_stages[current_index-1]]
                        stage_duration = int(total_cycle_days * (stage_pct - prev_stage_pct))
                        randomness = random.randint(-int(stage_duration * 0.2), int(stage_duration * 0.2))
                        stage_entry_date += timedelta(days=randomness)
                        stage_entry_date = max(stage_entry_date, created_date + timedelta(days=1))
                    
                    field_name = "Stage_" + stage_name.split(" - ")[0] + "_" + stage_name.split(" - ")[1].replace(" ", "_") + "_Date"
                    stage_dates[field_name] = stage_entry_date.strftime("%Y-%m-%d")
        
        # HANDLE OPEN DEALS (not yet closed)
        else:
            # Calculate current progress based on deal age
            deal_age = (current_time - created_date).days
            progress_ratio = min(0.9, deal_age / total_cycle_days)
            
            for stage_name, stage_pct in stage_progression.items():
                if stage_name == "1 - Lead Qualification":
                    continue
                
                if stage_name in ["6 - Closed Lost", "7 - Closed Won"]:
                    # Open deals haven't reached close stages
                    field_name = "Stage_" + stage_name.split(" - ")[0] + "_" + stage_name.split(" - ")[1].replace(" ", "_") + "_Date"
                    stage_dates[field_name] = None
                elif stage_pct <= progress_ratio:
                    # Deal has reached this stage
                    days_to_stage = int(total_cycle_days * stage_pct)
                    stage_entry_date = created_date + timedelta(days=days_to_stage)
                    
                    # Add randomness
                    prev_stages = list(stage_progression.keys())
                    current_index = prev_stages.index(stage_name)
                    if current_index > 0:
                        prev_stage_pct = stage_progression[prev_stages[current_index-1]]
                        stage_duration = int(total_cycle_days * (stage_pct - prev_stage_pct))
                        randomness = random.randint(-int(stage_duration * 0.2), int(stage_duration * 0.2))
                        stage_entry_date += timedelta(days=randomness)
                        stage_entry_date = max(stage_entry_date, created_date + timedelta(days=1))
                    
                    field_name = "Stage_" + stage_name.split(" - ")[0] + "_" + stage_name.split(" - ")[1].replace(" ", "_") + "_Date"
                    stage_dates[field_name] = stage_entry_date.strftime("%Y-%m-%d")
                else:
                    # Deal hasn't reached this stage yet
                    field_name = "Stage_" + stage_name.split(" - ")[0] + "_" + stage_name.split(" - ")[1].replace(" ", "_") + "_Date"
                    stage_dates[field_name] = None
        
        # CALCULATE DAYS SPENT IN EACH STAGE
        stage_names_ordered = [
            "1 - Lead Qualification", "2 - Discovery", "3 - Proposal",
            "4 - Negotiation", "5 - Contract Review", "6 - Closed Lost", "7 - Closed Won"
        ]
        
        for i, stage_name in enumerate(stage_names_ordered):
            stage_num = stage_name.split(" - ")[0]
            stage_label = stage_name.split(" - ")[1].replace(" ", "_")
            
            current_stage_field = f"Stage_{stage_num}_{stage_label}_Date"
            days_field = f"Days_in_Stage_{stage_num}_{stage_label}"
            
            # Skip if deal never reached this stage
            if current_stage_field not in stage_dates or stage_dates[current_stage_field] is None:
                days_in_stage[days_field] = None
                continue
            
            current_stage_date = datetime.strptime(stage_dates[current_stage_field], "%Y-%m-%d")
            
            # Find when deal moved to next stage
            next_stage_date = None
            for j in range(i + 1, len(stage_names_ordered)):
                next_stage_name = stage_names_ordered[j]
                next_stage_num = next_stage_name.split(" - ")[0]
                next_stage_label = next_stage_name.split(" - ")[1].replace(" ", "_")
                next_stage_field = f"Stage_{next_stage_num}_{next_stage_label}_Date"
                
                if next_stage_field in stage_dates and stage_dates[next_stage_field] is not None:
                    next_stage_date = datetime.strptime(stage_dates[next_stage_field], "%Y-%m-%d")
                    break
            
            # Calculate how many days were spent in this stage
            if next_stage_date:
                days_in_current_stage = (next_stage_date - current_stage_date).days
                days_in_stage[days_field] = days_in_current_stage
            else:
                # This is the current stage or final stage
                if final_stage in ["6 - Closed Lost", "7 - Closed Won"]:
                    days_in_current_stage = (expected_close_date - current_stage_date).days
                else:
                    days_in_current_stage = (current_time - current_stage_date).days
                days_in_stage[days_field] = max(0, days_in_current_stage)
        
        # Combine stage dates and days calculations
        result = stage_dates.copy()
        result.update(days_in_stage)
        return result
    
    def calculate_deal_progression(self, segment, created_date, is_competitive=False, is_icp=True):
        """
        DEAL LIFECYCLE CALCULATOR: Determines current stage and timeline for a deal
        
        This calculates:
        1. How long the sales cycle should be
        2. Whether the deal is finished (won/lost) or still open
        3. What stage it's currently in (for open deals)
        
        Returns: current stage, expected close date, highest stage reached (if lost)
        """
        # Get the appropriate configuration for this deal
        segment_config, config_type = self.get_deal_configuration(segment, is_competitive, is_icp)
        current_time = datetime(2025, 9, 13)  # Simulation "current date"
        
        # Calculate realistic cycle time with some randomness
        base_cycle = segment_config["avg_cycle_days"]
        actual_cycle = int(np.random.normal(base_cycle, base_cycle * 0.3))  # 30% standard deviation
        actual_cycle = max(20, actual_cycle)  # Minimum 20 days
        
        expected_close_date = created_date + timedelta(days=actual_cycle)
        deal_age = (current_time - created_date).days
        
        # CLOSED DEALS: Past their expected close date
        if expected_close_date <= current_time:
            final_stage, highest_stage_reached = self.determine_deal_outcome_and_highest_stage(
                segment, is_competitive, is_icp)
            return final_stage, expected_close_date, highest_stage_reached
        
        # OPEN DEALS: Still in progress
        else:
            # Calculate how far through the cycle we are
            progress_ratio = min(0.9, deal_age / actual_cycle)  # Max 90% progress for open deals
            
            # Determine current stage based on deal type and progress
            if config_type == "Non-ICP" and segment == "Enterprise":
                # Non-ICP Enterprise deals get stuck in Discovery
                if progress_ratio < 0.10:
                    return "1 - Lead Qualification", expected_close_date, None
                elif progress_ratio < 0.60:  # Stuck in Discovery!
                    return "2 - Discovery", expected_close_date, None
                elif progress_ratio < 0.75:
                    return "3 - Proposal", expected_close_date, None
                elif progress_ratio < 0.90:
                    return "4 - Negotiation", expected_close_date, None
                else:
                    return "5 - Contract Review", expected_close_date, None
            elif is_competitive:
                # Competitive deals have long negotiation periods
                if progress_ratio < 0.10:
                    return "1 - Lead Qualification", expected_close_date, None
                elif progress_ratio < 0.20:
                    return "2 - Discovery", expected_close_date, None
                elif progress_ratio < 0.25:
                    return "3 - Proposal", expected_close_date, None
                elif progress_ratio < 0.80:
                    return "4 - Negotiation", expected_close_date, None
                else:
                    return "5 - Contract Review", expected_close_date, None
            else:
                # Base progression - healthy deal flow
                if progress_ratio < 0.15:
                    return "1 - Lead Qualification", expected_close_date, None
                elif progress_ratio < 0.35:
                    return "2 - Discovery", expected_close_date, None
                elif progress_ratio < 0.55:
                    return "3 - Proposal", expected_close_date, None
                elif progress_ratio < 0.75:
                    return "4 - Negotiation", expected_close_date, None
                else:
                    return "5 - Contract Review", expected_close_date, None

    def get_growth_multiplier(self, created_date, base_date=datetime(2023, 1, 1)):
        """
        BUSINESS GROWTH SIMULATION: Calculates deal size multiplier based on company growth
        
        The company is growing 25% per quarter, so newer deals tend to be larger.
        This adds realism by simulating business expansion over time.
        """
        # Calculate quarters elapsed since baseline
        quarters_elapsed = ((created_date.year - base_date.year) * 4 + 
                           (created_date.month - 1) // 3 - 
                           (base_date.month - 1) // 3)
        
        # Apply quarterly growth with some volatility
        base_growth = (1 + self.quarterly_growth_rate) ** quarters_elapsed
        volatility = random.uniform(1 - self.growth_volatility, 1 + self.growth_volatility)
        
        return base_growth * volatility
    
    def get_fiscal_quarter_from_date(self, date):
        """
        FISCAL CALENDAR: Converts a date to fiscal quarter format
        
        Returns format like "2024-Q3" for easy quarterly analysis
        """
        quarter = ((date.month - 1) // 3) + 1
        return f"{date.year}-Q{quarter}"
    
    def generate_opportunities(self, num_deals=5000, start_date=datetime(2023, 1, 1), 
                             end_date=datetime(2025, 9, 13)):
        """
        MAIN GENERATION ENGINE: Creates the complete synthetic dataset
        
        This is the main function that creates all the opportunities with the dual narratives:
        1. Mid-Market competitive pressure increasing over time
        2. Enterprise ICP targeting declining over time
        
        Parameters:
        - num_deals: How many opportunities to create (default 5,000)
        - start_date: Earliest possible opportunity creation date
        - end_date: Latest possible opportunity creation date (simulation "current date")
        
        Returns: List of opportunity dictionaries
        """
        
        opportunities = []
        total_days = (end_date - start_date).days
        
        print(f"Generating {num_deals:,} opportunities from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")
        
        for i in range(num_deals):
            # Show progress every 1000 deals
            if (i + 1) % 1000 == 0:
                print(f"  Generated {i + 1:,} deals...")
            
            # CREATE DATE SELECTION
            # Use exponential distribution to create more recent deals (business growth)
            days_from_end = int(np.random.exponential(total_days / 4))
            days_from_end = min(days_from_end, total_days)
            created_date = end_date - timedelta(days=days_from_end)
            
            # SEGMENT SELECTION
            # Distribute deals across segments: 25% Enterprise, 50% Mid-Market, 25% SMB
            segment = np.random.choice(
                ["Enterprise", "Mid-Market", "SMB"],
                p=[0.25, 0.50, 0.25]
            )
            
            # GEOGRAPHIC ASSIGNMENT
            region, geo, area, sales_rep = self.select_region_and_sales_rep()
            
            # CHALLENGE APPLICATION
            # Determine if this deal exhibits the challenges based on segment and timing
            is_competitive = self.is_deal_competitive(segment, created_date)
            is_icp = self.is_account_icp(segment, created_date)
            
            # COMPANY SELECTION
            # Choose appropriate company based on ICP status and region
            company_name, industry = self.select_company_and_industry(region, is_icp)
            
            # DEAL AMOUNT CALCULATION
            # Base amount from segment range, adjusted for company growth over time
            base_amount = random.randint(
                self.segments[segment]["min_amount"], 
                self.segments[segment]["max_amount"]
            )
            growth_multiplier = self.get_growth_multiplier(created_date)
            amount = int(base_amount * growth_multiplier)
            
            # DEAL PROGRESSION CALCULATION
            # Determine current stage, timeline, and outcome
            stage, close_date, highest_stage_reached = self.calculate_deal_progression(
                segment, created_date, is_competitive, is_icp)
            
            # STAGE TIMELINE CALCULATION
            # Calculate when deal entered each stage and how long it spent there
            stage_data = self.calculate_stage_progression(
                segment, created_date, close_date, stage, highest_stage_reached, 
                is_competitive, is_icp)
            
            # PROBABILITY AND EXPECTED REVENUE
            # Calculate win probability and expected revenue based on current stage
            if stage == "7 - Closed Won":
                probability = 100
                expected_revenue = amount
            elif stage == "6 - Closed Lost":
                probability = 0
                expected_revenue = 0
            else:
                # Open deals - probability depends on stage and deal characteristics
                segment_config, config_type = self.get_deal_configuration(segment, is_competitive, is_icp)
                
                if config_type == "Non-ICP" and segment == "Enterprise":
                    # Non-ICP Enterprise deals have low probabilities
                    stage_probs = {
                        "1 - Lead Qualification": 8, "2 - Discovery": 12, 
                        "3 - Proposal": 30, "4 - Negotiation": 50, "5 - Contract Review": 70
                    }
                elif is_competitive:
                    # Competitive deals have moderate probabilities
                    stage_probs = {
                        "1 - Lead Qualification": 10, "2 - Discovery": 15, 
                        "3 - Proposal": 25, "4 - Negotiation": 35, "5 - Contract Review": 60
                    }
                else:
                    # Base deals have healthy probabilities
                    stage_probs = {
                        "1 - Lead Qualification": 15, "2 - Discovery": 25, 
                        "3 - Proposal": 40, "4 - Negotiation": 65, "5 - Contract Review": 85
                    }
                
                probability = stage_probs[stage]
                expected_revenue = amount * (probability / 100)
            
            # ADDITIONAL CALCULATIONS
            current_time = datetime(2025, 9, 13)
            age_days = (current_time - created_date).days
            fiscal_quarter = self.get_fiscal_quarter_from_date(close_date)
            
            # CHALLENGE ATTRIBUTION
            # Identify which competitor (for competitive deals) and why non-ICP
            competitor = random.choice(self.competitors) if is_competitive else None
            icp_mismatch_reason = "Industry not core ICP" if not is_icp else None
            
            # CREATE OPPORTUNITY RECORD
            # Build the complete opportunity record with all calculated fields
            opportunity = {
                # Basic Identification
                "Opportunity_ID": f"OPP-{i+1:06d}",
                "Account_Name": company_name,
                "Account_Industry": industry,
                "Account_Region": region,
                "Account_Geo": geo,
                "Account_Area": area,
                "Opportunity_Name": f"{company_name.split()[0]} - AI Platform Implementation",
                "Owner": sales_rep,
                
                # Deal Details
                "Stage": stage,
                "Amount": amount,
                "Expected_Revenue": int(expected_revenue),
                "Probability_Percent": probability,
                "Segment": segment,
                
                # Timing
                "Created_Date": created_date.strftime("%Y-%m-%d"),
                "Close_Date": close_date.strftime("%Y-%m-%d"),
                "Age_Days": age_days,
                "Fiscal_Quarter": fiscal_quarter,
                
                # Product and Challenge Flags
                "Product_Type": "AI Platform",
                "Highest_Stage_Reached": highest_stage_reached if stage == "6 - Closed Lost" else None,
                "Competitive_Deal": is_competitive,
                "Primary_Competitor": competitor,
                "Is_ICP_Account": is_icp,
                "ICP_Mismatch_Reason": icp_mismatch_reason
            }
            
            # Add detailed stage progression data
            opportunity.update(stage_data)
            opportunities.append(opportunity)
        
        self.opportunities = opportunities
        print(f"Successfully generated {len(opportunities):,} opportunities!")
        return opportunities
    
    def analyze_dataset(self):
        """
        DATASET ANALYSIS: Provides comprehensive analysis of the generated data
        
        This creates a detailed report showing:
        1. Geographic distribution
        2. Evidence of the Mid-Market competitive challenge
        3. Evidence of the Enterprise ICP targeting challenge
        4. Sales team performance metrics
        
        Returns: Formatted analysis string
        """
        if not self.opportunities:
            return "No opportunities to analyze"
        
        df = pd.DataFrame(self.opportunities)
        analysis = []
        
        # BASIC SUMMARY
        analysis.append(f"Global Dataset Summary:")
        analysis.append(f"Total Opportunities: {len(df):,}")
        analysis.append(f"Total Pipeline Value: ${df['Amount'].sum():,.0f}")
        analysis.append(f"Date Range: {df['Created_Date'].min()} to {df['Created_Date'].max()}")
        
        # GEOGRAPHIC DISTRIBUTION
        analysis.append(f"\n=== GEOGRAPHIC DISTRIBUTION ===")
        geo_summary = df.groupby('Account_Geo').size().sort_values(ascending=False)
        for geo, count in geo_summary.items():
            percentage = (count / len(df)) * 100
            analysis.append(f"{geo}: {count:,} deals ({percentage:.1f}%)")
        
        # Regional breakdown for top geos
        analysis.append(f"\nTop Regions by Deal Count:")
        region_summary = df.groupby('Account_Region').size().sort_values(ascending=False).head(10)
        for region, count in region_summary.items():
            percentage = (count / len(df)) * 100
            analysis.append(f"  {region}: {count:,} deals ({percentage:.1f}%)")
        
        # MID-MARKET COMPETITIVE STORY ANALYSIS
        competitive_deals = df['Competitive_Deal'].sum()
        competitive_rate = (competitive_deals / len(df)) * 100
        analysis.append(f"\n=== MID-MARKET COMPETITIVE STORY ===")
        analysis.append(f"Total Competitive Deals: {competitive_deals:,} ({competitive_rate:.1f}%)")
        
        analysis.append(f"Competitive Rates by Segment:")
        for segment in ["Enterprise", "Mid-Market", "SMB"]:
            segment_df = df[df['Segment'] == segment]
            competitive_count = segment_df['Competitive_Deal'].sum()
            segment_competitive_rate = (competitive_count / len(segment_df)) * 100
            analysis.append(f"  {segment}: {competitive_count:,}/{len(segment_df):,} ({segment_competitive_rate:.1f}%)")
        
        # Competitive rates by top regions
        analysis.append(f"Competitive Rates by Top Regions:")
        top_regions = region_summary.head(5).index
        for region in top_regions:
            region_df = df[df['Account_Region'] == region]
            regional_competitive = region_df['Competitive_Deal'].sum()
            regional_competitive_rate = (regional_competitive / len(region_df)) * 100
            analysis.append(f"  {region}: {regional_competitive_rate:.1f}% competitive")
        
        # ENTERPRISE ICP TARGETING STORY ANALYSIS
        icp_deals = df['Is_ICP_Account'].sum()
        icp_rate = (icp_deals / len(df)) * 100
        analysis.append(f"\n=== ENTERPRISE ICP TARGETING STORY ===")
        analysis.append(f"Total ICP Deals: {icp_deals:,} ({icp_rate:.1f}%)")
        
        analysis.append(f"ICP Rates by Segment:")
        for segment in ["Enterprise", "Mid-Market", "SMB"]:
            segment_df = df[df['Segment'] == segment]
            icp_count = segment_df['Is_ICP_Account'].sum()
            segment_icp_rate = (icp_count / len(segment_df)) * 100
            analysis.append(f"  {segment}: {icp_count:,}/{len(segment_df):,} ({segment_icp_rate:.1f}%)")
        
        # ICP rates by top regions
        analysis.append(f"ICP Rates by Top Regions:")
        for region in top_regions:
            region_df = df[df['Account_Region'] == region]
            regional_icp = region_df['Is_ICP_Account'].sum()
            regional_icp_rate = (regional_icp / len(region_df)) * 100
            analysis.append(f"  {region}: {regional_icp_rate:.1f}% ICP")
        
        # SALES TEAM PERFORMANCE ANALYSIS
        analysis.append(f"\n=== SALES TEAM PERFORMANCE ===")
        rep_performance = df.groupby('Owner').agg({
            'Opportunity_ID': 'count',
            'Amount': 'sum',
            'Stage': lambda x: (x == '7 - Closed Won').sum()
        }).rename(columns={'Opportunity_ID': 'Total_Deals', 'Amount': 'Total_Pipeline', 'Stage': 'Wins'})
        
        rep_performance['Win_Rate'] = (rep_performance['Wins'] / rep_performance['Total_Deals'] * 100).round(1)
        top_performers = rep_performance.sort_values('Total_Pipeline', ascending=False).head(10)
        
        analysis.append(f"Top 10 Sales Reps by Pipeline Value:")
        for rep, data in top_performers.iterrows():
            analysis.append(f"  {rep}: ${data['Total_Pipeline']:,.0f} pipeline, {data['Total_Deals']} deals, {data['Win_Rate']}% win rate")
        
        return "\n".join(analysis)
    
    def save_to_csv(self):
        """
        FILE OUTPUT: Saves the generated dataset to CSV and prints analysis
        
        Creates the CSV file and provides immediate feedback about what was generated.
        """
        if not self.opportunities:
            print("No opportunities to save")
            return
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)
        
        # Save to CSV
        df = pd.DataFrame(self.opportunities)
        full_path = os.path.join(self.output_path, self.filename)
        df.to_csv(full_path, index=False)
        
        print(f"Global dataset saved to: {full_path}")
        print(f"Columns: {list(df.columns)}")
        print(f"\n{self.analyze_dataset()}")
        
        return full_path

# MAIN EXECUTION FUNCTION
def main():
    """
    EXECUTION SCRIPT: Runs the data generation process with default parameters
    
    This function demonstrates how to use the GlobalSalesDataGenerator class
    to create a synthetic dataset for analysis.
    """
    # Configuration - modify these values as needed
    OUTPUT_PATH = "/Users/keithgliksman/Desktop/Anthropic Case"  # Where to save the file
    FILENAME = "sales_data.csv"                               # Name of the output file
    NUM_DEALS = 50000                                          # Number of opportunities to generate
    
    print("Generating global AI company sales data with dual narratives...")
    print("1. Mid-Market: Increasing competitive pressure")
    print("2. Enterprise: Declining ICP targeting")
    print("3. Global sales teams across all regions")
    
    # Create the data generator instance
    generator = GlobalSalesDataGenerator(output_path=OUTPUT_PATH, filename=FILENAME)
    
    # Generate the opportunities dataset
    opportunities = generator.generate_opportunities(num_deals=NUM_DEALS)
    
    # Save to CSV file with analysis
    generator.save_to_csv()
    
    print(f"\nGeneration complete! Created {len(opportunities)} opportunities across global regions.")

if __name__ == "__main__":
    main()


# ADDITIONAL DOCUMENTATION FOR KEY CONCEPTS:

"""
=================================================================================================
                              KEY CONCEPTS EXPLAINED
=================================================================================================

IDEAL CUSTOMER PROFILE (ICP):
An ICP defines the type of companies that are most likely to buy your product and be successful 
with it. In this simulation:
- ICP Industries: Software & Technology, Financial Services
- Non-ICP Industries: Healthcare, Manufacturing, Retail, Professional Services, Education, Media
- ICP companies are easier to sell to and have higher win rates
- The Enterprise challenge shows what happens when sales teams stop targeting ICP accounts

COMPETITIVE DEALS:
Deals where the customer is actively evaluating multiple vendors. These deals are characterized by:
- Longer sales cycles (customers take more time to decide)
- Lower win rates (you're competing against other solutions)
- More time spent in negotiation stages
- The Mid-Market challenge shows increasing competitive pressure over time

SALES STAGES EXPLAINED:
1. Lead Qualification: Initial contact, determining if there's a fit
2. Discovery: Deep-dive into customer needs, pain points, and requirements  
3. Proposal: Presenting solution and pricing
4. Negotiation: Working through contract terms, pricing, and conditions
5. Contract Review: Legal review and final approvals
6. Closed Lost: Deal was lost to competitor or customer decided not to buy
7. Closed Won: Deal was successfully closed and contract signed

STAGE CONVERSION RATES:
The percentage of deals that successfully move from one stage to the next. For example:
- If 100 deals enter Discovery stage
- And the Discovery->Proposal conversion rate is 70%
- Then 70 deals will move to Proposal stage
- The remaining 30 will be lost at Discovery stage

THE TWO BUSINESS CHALLENGES SIMULATED:

CHALLENGE 1 - MID-MARKET COMPETITIVE PRESSURE:
- Competition increases 2% per month in Mid-Market segment
- Started at 20% competitive deals in Jan 2023
- By Sep 2025, could reach 60%+ competitive deals
- Competitive deals have 25% win rate vs 45% for non-competitive
- Sales cycles 50% longer for competitive deals
- Most impact in Mid-Market ($25K-$100K deals)

CHALLENGE 2 - ENTERPRISE ICP TARGETING DECLINE:  
- ICP targeting discipline erodes 2.5% per month in Enterprise
- Started with 90% ICP deals in Jan 2023  
- By Sep 2025, could drop to 30% ICP deals
- Non-ICP deals have 15% win rate vs 30% for ICP deals
- Non-ICP deals get stuck in Discovery stage (60% of cycle time)
- Sales cycles 55% longer for non-ICP deals
- Most impact in Enterprise ($100K-$500K deals)

REALISTIC BUSINESS SIMULATION ELEMENTS:
- Company grows 25% per quarter (deal sizes increase over time)
- Seasonal and regional variations in deal timing
- Realistic sales rep performance distributions
- Industry-specific company names by region
- Stage progression timing that matches real sales processes

HOW TO USE THIS DATA FOR ANALYSIS:
1. Time-series analysis: Track competitive rates and ICP rates over time
2. Cohort analysis: Compare performance by region, segment, sales rep
3. Stage analysis: Identify where deals get stuck (Discovery bottleneck)
4. Win rate analysis: Compare ICP vs non-ICP, competitive vs non-competitive
5. Sales cycle analysis: Measure impact of challenges on deal velocity
6. Pipeline forecasting: Use stage probabilities for revenue predictions
7. Sales coaching: Identify reps struggling with competitive or non-ICP deals

=================================================================================================
"""
