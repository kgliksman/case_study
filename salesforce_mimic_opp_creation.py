import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

class SalesforceDataGenerator:
    """
    Generates synthetic Salesforce Opportunity data that mimics real-world patterns
    with both evergreen characteristics and specific event-driven changes.
    """
    
    def __init__(self, output_path="./", filename="synthetic_opportunities.csv"):
        """
        Initialize the data generator with output configuration.
        
        Args:
            output_path (str): Directory path where CSV will be saved
            filename (str): Name of the output CSV file
        """
        self.output_path = output_path
        self.filename = filename
        self.opportunities = []
        
        # Configuration: Sales Stages (with numbers for ordering)
        self.stages = [
            "1 - Lead Qualification",
            "2 - Discovery", 
            "3 - Proposal Development",
            "4 - Negotiation",
            "5 - Contract Review",
            "6 - Closed Lost",
            "7 - Closed Won"
        ]
        
        # Configuration: Geographic structure with regional sales teams
        self.geo_structure = {
            "North America": {
                "countries": ["United States", "Canada", "Mexico"],
                "sub_regions": ["US West", "US East", "US Central", "Canada", "Mexico"],
                "reps": {
                    "Sarah Chen": {"experience": "senior", "specialty": "enterprise", "skill_multiplier": 1.3, "location": "US West"},
                    "Mike Rodriguez": {"experience": "senior", "specialty": "enterprise", "skill_multiplier": 1.2, "location": "US East"},
                    "Alex Thompson": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.9, "location": "US Central"},
                    "Jordan Kim": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.85, "location": "Canada"},
                    "Taylor Brown": {"experience": "mid", "specialty": "general", "skill_multiplier": 0.7, "location": "Mexico"},  # Underperformer
                    "Jamie Wilson": {"experience": "mid", "specialty": "mid_market", "skill_multiplier": 1.1, "location": "US West"},
                    "Chris Davis": {"experience": "senior", "specialty": "api", "skill_multiplier": 1.25, "location": "US East"},
                    "Morgan Lee": {"experience": "mid", "specialty": "enterprise", "skill_multiplier": 1.0, "location": "US Central"},
                    "Casey Martinez": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.88, "location": "Mexico"}
                }
            },
            "Europe": {
                "countries": ["United Kingdom", "Germany", "France", "Netherlands", "Sweden", "Spain"],
                "sub_regions": ["UK & Ireland", "DACH", "France", "Nordics", "Southern Europe", "Benelux"],
                "reps": {
                    "Emma Clarke": {"experience": "senior", "specialty": "enterprise", "skill_multiplier": 1.25, "location": "UK & Ireland"},
                    "Hans Mueller": {"experience": "senior", "specialty": "enterprise", "skill_multiplier": 1.15, "location": "DACH"},
                    "Sophie Dubois": {"experience": "mid", "specialty": "mid_market", "skill_multiplier": 1.05, "location": "France"},
                    "Erik Larsson": {"experience": "mid", "specialty": "general", "skill_multiplier": 1.0, "location": "Nordics"},
                    "Marco Rossi": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.82, "location": "Southern Europe"},  # Underperformer
                    "Anna van Berg": {"experience": "mid", "specialty": "api", "skill_multiplier": 1.12, "location": "Benelux"},
                    "James O'Connor": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.87, "location": "UK & Ireland"},
                }
            },
            "Asia Pacific": {
                "countries": ["Japan", "Australia", "Singapore", "India", "South Korea"],
                "sub_regions": ["Japan", "Australia & NZ", "Southeast Asia", "India", "Korea"],
                "reps": {
                    "Yuki Tanaka": {"experience": "senior", "specialty": "enterprise", "skill_multiplier": 1.2, "location": "Japan"},
                    "Oliver Smith": {"experience": "senior", "specialty": "mid_market", "skill_multiplier": 1.18, "location": "Australia & NZ"},
                    "Priya Sharma": {"experience": "mid", "specialty": "api", "skill_multiplier": 1.08, "location": "India"},
                    "Li Wei": {"experience": "mid", "specialty": "general", "skill_multiplier": 0.95, "location": "Southeast Asia"},
                    "Kim Min-jun": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.9, "location": "Korea"},
                    "Raj Patel": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.75, "location": "India"}  # Underperformer
                }
            },
            "Latin America": {
                "countries": ["Brazil", "Argentina", "Colombia", "Chile"],
                "sub_regions": ["Brazil", "Argentina", "Colombia", "Chile"],
                "reps": {
                    "Carlos Silva": {"experience": "mid", "specialty": "enterprise", "skill_multiplier": 1.1, "location": "Brazil"},
                    "Isabella Garcia": {"experience": "mid", "specialty": "mid_market", "skill_multiplier": 1.05, "location": "Argentina"},
                    "Diego Morales": {"experience": "junior", "specialty": "general", "skill_multiplier": 0.85, "location": "Colombia"},
                    "Camila Rodriguez": {"experience": "junior", "specialty": "smb", "skill_multiplier": 0.8, "location": "Chile"}
                }
            }
        }
        
        # Flatten reps for easy access
        self.reps = {}
        for region, data in self.geo_structure.items():
            for rep_name, rep_info in data["reps"].items():
                rep_info["region"] = region
                self.reps[rep_name] = rep_info
        
        # Configuration: Account segments and characteristics
        self.segments = {
            "Enterprise": {
                "min_amount": 100000, "max_amount": 2000000, "avg_cycle_days": 180,
                "base_win_rate": 0.35, "complexity_factor": 1.5
            },
            "Mid-Market": {
                "min_amount": 25000, "max_amount": 150000, "avg_cycle_days": 90,
                "base_win_rate": 0.45, "complexity_factor": 1.0
            },
            "SMB": {
                "min_amount": 5000, "max_amount": 35000, "avg_cycle_days": 45,
                "base_win_rate": 0.25, "complexity_factor": 0.7
            }
        }
        
        # Configuration: Product types and characteristics
        self.products = {
            "API Enterprise": {"complexity": "high", "win_rate_modifier": 1.2, "cycle_modifier": 1.1},
            "Team Subscription": {"complexity": "low", "win_rate_modifier": 0.8, "cycle_modifier": 0.7},
            "Custom Enterprise": {"complexity": "very_high", "win_rate_modifier": 1.0, "cycle_modifier": 1.4},
            "Developer Tools": {"complexity": "medium", "win_rate_modifier": 1.1, "cycle_modifier": 0.9},
        }
        
        # Configuration: Baseline stage conversion rates (before events)
        self.base_conversion_rates = {
            "1 - Lead Qualification": 0.75,
            "2 - Discovery": 0.65,
            "3 - Proposal Development": 0.55,
            "4 - Negotiation": 0.70,
            "5 - Contract Review": 0.85,
        }
        
        # Configuration: Event timeline and impacts
        self.events = {
            "security_process_change": {
                "start_date": datetime(2024, 4, 1),  # Q2 2024
                "affected_stage": "5 - Contract Review",
                "conversion_impact": -0.15,  # Reduces conversion by 15%
                "velocity_impact": 1.4  # Increases time in stage by 40%
            },
            "pricing_model_change": {
                "start_date": datetime(2024, 7, 1),  # Q3 2024
                "affected_stage": "4 - Negotiation", 
                "conversion_impact": -0.20,  # Reduces conversion by 20%
                "velocity_impact": 1.3  # Increases time in stage by 30%
            },
            "competitive_pressure": {
                "start_date": datetime(2024, 6, 1),  # Mid-2024
                "affected_stage": "2 - Discovery",
                "conversion_impact": -0.10,  # Reduces conversion by 10%
                "velocity_impact": 1.25  # Increases time in stage by 25%
            }
        }
    
    def get_reps_for_region(self, region):
        """Get all sales reps for a specific region."""
        return [rep_name for rep_name, rep_info in self.reps.items() 
                if rep_info["region"] == region]
    
    def select_rep_for_account(self, region, segment):
        """Select appropriate rep based on region and segment."""
        available_reps = self.get_reps_for_region(region)
        
        # Filter by specialty if possible
        if segment == "Enterprise":
            specialized_reps = [rep for rep in available_reps 
                              if self.reps[rep]["specialty"] in ["enterprise", "general"]]
            if specialized_reps:
                available_reps = specialized_reps
        elif segment == "SMB":
            specialized_reps = [rep for rep in available_reps 
                              if self.reps[rep]["specialty"] in ["smb", "general"]]
            if specialized_reps:
                available_reps = specialized_reps
        
        return random.choice(available_reps)
    
    def generate_company_names(self, count=1000):
        """Generate realistic company names by region."""
        # Base company components
        prefixes = ["Tech", "Data", "Cloud", "Global", "Digital", "Smart", "Next", "Future", "Pro", "Elite"]
        suffixes = ["Solutions", "Systems", "Corp", "Industries", "Enterprises", "Group", "Labs", "Works", "Tech", "Inc"]
        
        # Regional variations
        regional_prefixes = {
            "North America": ["American", "US", "National", "Continental"],
            "Europe": ["European", "Euro", "Nordic", "Alpine", "Continental"],
            "Asia Pacific": ["Asia", "Pacific", "Oriental", "Dragon", "Rising"],
            "Latin America": ["Latino", "South", "Americas", "Tropical"]
        }
        
        companies = {}
        for region in self.geo_structure.keys():
            regional_companies = []
            region_prefixes = prefixes + regional_prefixes[region]
            
            for _ in range(count // 4):  # Distribute evenly across regions
                if random.random() < 0.3:  # 30% single word companies
                    name = f"{random.choice(region_prefixes)}{random.choice(['Corp', 'Inc', 'LLC', 'Co'])}"
                else:
                    name = f"{random.choice(region_prefixes)} {random.choice(suffixes)}"
                regional_companies.append(name)
            
            companies[region] = regional_companies
        
        return companies
    
    def get_seasonal_multiplier(self, date):
        """Apply seasonal patterns to deal creation and conversion."""
        month = date.month
        
        # Q1: Budget flush (more deals, but lower quality)
        if month in [1, 2, 3]:
            return {"volume": 1.3, "win_rate": 0.9}
        # Q2: Normal operations
        elif month in [4, 5, 6]:
            return {"volume": 1.0, "win_rate": 1.0}
        # Q3: Summer slowdown
        elif month in [7, 8, 9]:
            return {"volume": 0.8, "win_rate": 1.05}
        # Q4: Year-end push
        else:
            return {"volume": 1.2, "win_rate": 1.1}
    
    def get_event_impact(self, stage, date):
        """Calculate the impact of discrete events on stage performance."""
        conversion_modifier = 1.0
        velocity_modifier = 1.0
        
        for event_name, event_config in self.events.items():
            if date >= event_config["start_date"] and stage == event_config["affected_stage"]:
                conversion_modifier *= (1 + event_config["conversion_impact"])
                velocity_modifier *= event_config["velocity_impact"]
        
        return conversion_modifier, velocity_modifier
    
    def calculate_stage_progression(self, segment, product, rep, created_date, region):
        """
        Simulate how an opportunity progresses through sales stages.
        Returns list of stage transitions with dates and outcomes.
        """
        progression = []
        current_date = created_date
        current_stage_index = 0
        
        # Get base characteristics
        segment_config = self.segments[segment]
        product_config = self.products[product]
        rep_config = self.reps[rep]
        
        # Calculate base cycle time and win probability
        base_cycle = segment_config["avg_cycle_days"]
        base_win_rate = segment_config["base_win_rate"]
        
        # Apply modifiers
        cycle_modifier = product_config["cycle_modifier"] * (2.0 - rep_config["skill_multiplier"])
        win_rate_modifier = product_config["win_rate_modifier"] * rep_config["skill_multiplier"]
        
        # Regional complexity modifiers
        regional_modifiers = {
            "North America": {"cycle": 1.0, "complexity": 1.0},
            "Europe": {"cycle": 1.15, "complexity": 1.1},  # GDPR, multiple languages
            "Asia Pacific": {"cycle": 1.25, "complexity": 1.2},  # Cultural complexity
            "Latin America": {"cycle": 1.1, "complexity": 1.05}  # Economic volatility
        }
        
        regional_mod = regional_modifiers[region]
        cycle_modifier *= regional_mod["cycle"]
        win_rate_modifier *= (2.0 - regional_mod["complexity"]) / 1.0
        
        while current_stage_index < len(self.stages) - 2:  # Don't process final "Closed" stages
            current_stage = self.stages[current_stage_index]
            
            # Get seasonal and event impacts
            seasonal = self.get_seasonal_multiplier(current_date)
            conversion_impact, velocity_impact = self.get_event_impact(current_stage, current_date)
            
            # Calculate conversion probability for this stage
            base_conversion = self.base_conversion_rates[current_stage]
            stage_conversion = base_conversion * win_rate_modifier * seasonal["win_rate"] * conversion_impact
            stage_conversion = max(0.1, min(0.95, stage_conversion))  # Keep within realistic bounds
            
            # Calculate time spent in this stage
            avg_stage_time = (base_cycle / (len(self.stages) - 2)) * cycle_modifier * velocity_impact
            stage_days = max(1, int(np.random.exponential(avg_stage_time)))
            
            # Record this stage
            progression.append({
                "stage": current_stage,
                "date": current_date,
                "days_in_stage": stage_days,
                "conversion_probability": stage_conversion
            })
            
            # Determine if opportunity advances or is lost
            if random.random() <= stage_conversion:
                current_stage_index += 1
                current_date += timedelta(days=stage_days)
            else:
                # Deal is lost at this stage
                final_stage = "6 - Closed Lost"
                progression.append({
                    "stage": final_stage,
                    "date": current_date + timedelta(days=stage_days),
                    "days_in_stage": 0,
                    "outcome": "Closed Lost"
                })
                break
        else:
            # Deal made it through all stages - it's won
            final_stage = "7 - Closed Won"
            progression.append({
                "stage": final_stage,
                "date": current_date,
                "days_in_stage": 0,
                "outcome": "Closed Won"
            })
        
        return progression
    
    def generate_opportunities(self, num_opportunities=5000, start_date=None, end_date=None):
        """
        Generate the main dataset of opportunities.
        
        Args:
            num_opportunities (int): Number of opportunities to generate
            start_date (datetime): When to start generating opportunities
            end_date (datetime): When to stop generating opportunities
        """
        if start_date is None:
            start_date = datetime(2023, 1, 1)
        if end_date is None:
            end_date = datetime(2024, 12, 31)
        
        print(f"Generating {num_opportunities} opportunities from {start_date} to {end_date}")
        
        # Generate supporting data
        company_names_by_region = self.generate_company_names(2000)
        
        opportunities = []
        
        for i in range(num_opportunities):
            if i % 500 == 0:
                print(f"Progress: {i}/{num_opportunities} opportunities generated")
            
            # Generate basic opportunity characteristics
            created_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
            
            # Select region based on realistic distribution
            region = np.random.choice(
                list(self.geo_structure.keys()), 
                p=[0.45, 0.30, 0.15, 0.10]  # NA heavy, then EU, APAC, LATAM
            )
            
            # Select segment based on realistic distribution
            segment = np.random.choice(
                ["Enterprise", "Mid-Market", "SMB"], 
                p=[0.2, 0.35, 0.45]  # More SMB deals, fewer Enterprise
            )
            
            # Select rep based on region and segment
            rep = self.select_rep_for_account(region, segment)
            
            # Select product and amount based on segment
            segment_config = self.segments[segment]
            if segment == "Enterprise":
                product = np.random.choice(["API Enterprise", "Custom Enterprise"], p=[0.6, 0.4])
            elif segment == "SMB":
                product = np.random.choice(["Team Subscription", "Developer Tools"], p=[0.7, 0.3])
            else:  # Mid-Market
                product = random.choice(list(self.products.keys()))
            
            # Generate deal amount
            amount = random.randint(segment_config["min_amount"], segment_config["max_amount"])
            
            # Calculate stage progression
            progression = self.calculate_stage_progression(segment, product, rep, created_date, region)
            
            # Determine current state of the opportunity
            current_stage = progression[-1]["stage"]
            close_date = progression[-1]["date"]
            
            # Calculate probability and expected revenue based on current stage
            if "Closed" in current_stage:
                if "Won" in current_stage:
                    probability = 100
                    expected_revenue = amount
                else:
                    probability = 0
                    expected_revenue = 0
            else:
                # For open opportunities, use stage-based probability
                stage_num = int(current_stage.split(" - ")[0])
                probability = max(10, 100 - ((stage_num - 1) * 15))  # Decreasing probability by stage
                expected_revenue = amount * (probability / 100)
            
            # Calculate age
            age = (datetime.now() - created_date).days
            
            # Generate other fields
            company = random.choice(company_names_by_region[region])
            opportunity_name = f"{company} - {product} Implementation"
            
            # Get sub-region based on rep location
            sub_region = self.reps[rep]["location"]
            
            # Create opportunity record
            opportunity = {
                "Owner Role": "Account Executive",
                "Opportunity Owner": rep,
                "Account Name": company,
                "Opportunity Name": opportunity_name,
                "Stage": current_stage,
                "Amount": amount,
                "Expected Revenue": expected_revenue,
                "Probability (%)": probability,
                "Age": age,
                "Close Date": close_date.strftime("%Y-%m-%d"),
                "Created Date": created_date.strftime("%Y-%m-%d"),
                "Next Step": self.generate_next_step(current_stage),
                "Initial Source": np.random.choice(["Inbound Lead", "Outbound Prospecting", "Partner Referral", "Event"], 
                                                 p=[0.4, 0.35, 0.15, 0.1]),
                "Subscription Type": product,
                "Account Demographics: Geo": region,
                "Account Demographics: Region": sub_region,
                "Account Demographics: Area": segment,
                # Additional fields for analysis
                "Rep_Experience": self.reps[rep]["experience"],
                "Rep_Specialty": self.reps[rep]["specialty"],
                "Days_In_Current_Stage": progression[-2]["days_in_stage"] if len(progression) > 1 else 0,
            }
            
            opportunities.append(opportunity)
        
        self.opportunities = opportunities
        print(f"Generated {len(opportunities)} opportunities successfully!")
    
    def generate_next_step(self, stage):
        """Generate realistic next steps based on current stage."""
        next_steps = {
            "1 - Lead Qualification": ["Schedule discovery call", "Send qualification questionnaire", "Arrange demo"],
            "2 - Discovery": ["Complete needs assessment", "Technical deep dive", "Stakeholder meeting"],
            "3 - Proposal Development": ["Finalize proposal", "Present solution", "Submit pricing"],
            "4 - Negotiation": ["Address objections", "Finalize terms", "Executive approval"],
            "5 - Contract Review": ["Legal review", "Security approval", "Procurement process"],
            "6 - Closed Lost": ["Post-mortem analysis", "Future opportunity assessment"],
            "7 - Closed Won": ["Implementation kickoff", "Customer success handoff"]
        }
        return random.choice(next_steps.get(stage, ["Follow up"]))
    
    def save_to_csv(self):
        """Save the generated opportunities to CSV file."""
        if not self.opportunities:
            print("No opportunities generated. Please run generate_opportunities() first.")
            return
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_path, exist_ok=True)
        
        # Convert to DataFrame and save
        df = pd.DataFrame(self.opportunities)
        
        # Remove helper columns that aren't part of standard Salesforce export
        columns_to_remove = ["Rep_Experience", "Rep_Specialty", "Days_In_Current_Stage"]
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
        
        full_path = os.path.join(self.output_path, self.filename)
        df.to_csv(full_path, index=False)
        
        print(f"Dataset saved to: {full_path}")
        print(f"Dataset contains {len(df)} opportunities")
        print(f"Columns: {list(df.columns)}")
        
        # Print summary statistics
        print("\n=== DATASET SUMMARY ===")
        print(f"Date range: {df['Created Date'].min()} to {df['Created Date'].max()}")
        print(f"Total pipeline value: ${df['Amount'].sum():,.2f}")
        print(f"Average deal size: ${df['Amount'].mean():,.2f}")
        print("\nStage distribution:")
        print(df['Stage'].value_counts())
        
        # Calculate win rate
        total_closed = len(df[df['Stage'].str.contains('Closed')])
        won = len(df[df['Stage'] == '7 - Closed Won'])
        if total_closed > 0:
            win_rate = (won / total_closed) * 100
            print(f"\nOverall win rate: {win_rate:.1f}% ({won} won out of {total_closed} closed)")
        
        # Regional breakdown
        print("\nRegional distribution:")
        print(df['Account Demographics: Geo'].value_counts())
        
        # Rep performance summary
        print("\nTop performing reps (by pipeline value):")
        rep_performance = df.groupby('Opportunity Owner')['Amount'].sum().sort_values(ascending=False).head(10)
        for rep, value in rep_performance.items():
            print(f"  {rep}: ${value:,.0f}")

def main():
    """
    Main function to generate the synthetic Salesforce dataset.
    Modify the parameters below to customize your output.
    """
    
    # CONFIGURATION - Modify these parameters as needed
    OUTPUT_DIRECTORY = "/Users/keithgliksman/Desktop/Anthropic Case"  # Current directory - change this to your preferred path
    OUTPUT_FILENAME = "anthropic_opportunities_synthetic6.csv"  # Change filename as needed
    NUMBER_OF_OPPORTUNITIES = 5000
    START_DATE = datetime(2023, 1, 1)
    END_DATE = datetime(2024, 12, 31)
    
    # Generate the dataset
    print("Starting Enhanced Synthetic Salesforce Data Generation...")
    print("=" * 60)
    
    generator = SalesforceDataGenerator(
        output_path=OUTPUT_DIRECTORY,
        filename=OUTPUT_FILENAME
    )
    
    generator.generate_opportunities(
        num_opportunities=NUMBER_OF_OPPORTUNITIES,
        start_date=START_DATE,
        end_date=END_DATE
    )
    
    generator.save_to_csv()
    
    print("=" * 60)
    print("Data generation complete!")
    print(f"Your enhanced synthetic dataset is ready at: {os.path.join(OUTPUT_DIRECTORY, OUTPUT_FILENAME)}")
    
    # Print rep roster
    print("\n=== SALES REP ROSTER ===")
    for region, data in generator.geo_structure.items():
        print(f"\n{region}:")
        for rep_name, rep_info in data["reps"].items():
            print(f"  {rep_name} ({rep_info['experience']}, {rep_info['specialty']}) - {rep_info['location']}")

if __name__ == "__main__":
    main()
