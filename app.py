
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import uuid

# Import your existing database utilities
from db_utils import (
    init_db, initialize_admin_system, log_flight_search, log_enhanced_flight_search,
    fetch_recent_searches, fetch_all_searches, log_enhanced_contact, fetch_contacts,
    get_total_searches_count, get_recent_searches_count, get_monthly_searches,
    get_average_trip_duration, get_weekly_growth_rate, get_top_destinations,
    get_top_departures, get_budget_distribution, get_class_distribution,
    get_searches_over_time, get_flight_analytics, get_admin_summary_stats,
    generate_analytics_summary, log_event, backup_database, get_database_info
)

# Page configuration
st.set_page_config(
    page_title="RoamGenie Admin Dashboard",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
        .main-header {
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            padding: 1rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin-bottom: 2rem;
        }
        .metric-card {
            background: #f8f9fa;
            padding: 1rem;
            border-radius: 8px;
            border-left: 4px solid #667eea;
            margin: 0.5rem 0;
        }
        .sidebar-logo {
            text-align: center;
            padding: 1rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            color: white;
            margin-bottom: 1rem;
        }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
if "admin_logged_in" not in st.session_state:
    st.session_state.admin_logged_in = False
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# Admin credentials (move to environment variables in production)
ADMIN_CREDENTIALS = {
    "admin": "admin@123",
    "manager": "manager@123",
    "Webisdom": "admin@123"
}

def check_admin_credentials(username, password):
    """Check if admin credentials are correct"""
    return username in ADMIN_CREDENTIALS and ADMIN_CREDENTIALS[username] == password

def admin_login():
    """Admin login interface"""
    st.markdown('<div class="main-header"><h1>🔐 RoamGenie Admin Dashboard</h1></div>', 
                unsafe_allow_html=True)
    
    if not st.session_state.admin_logged_in:
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown("### Please Login to Continue")
            with st.form("admin_login_form"):
                username = st.text_input("👤 Username")
                password = st.text_input("🔒 Password", type="password")
                login_btn = st.form_submit_button("🔓 Login", use_container_width=True)
                
                if login_btn:
                    if check_admin_credentials(username, password):
                        st.session_state.admin_logged_in = True
                        st.session_state.admin_username = username
                        st.success("✅ Login successful!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid credentials!")
            
            # Demo credentials
            st.info("**Demo Credentials:**\n- Username: `admin` Password: `admin@123`")
        return False
    else:
        # Sidebar logout
        with st.sidebar:
            st.markdown(f'<div class="sidebar-logo"><h3>Welcome<br>{st.session_state.admin_username}</h3></div>', 
                       unsafe_allow_html=True)
            if st.button("🚪 Logout", use_container_width=True):
                st.session_state.admin_logged_in = False
                st.rerun()
        return True

def display_overview_metrics():
    """Display comprehensive overview metrics"""
    st.markdown("## 📊 Key Performance Indicators")
    
    try:
        stats = get_admin_summary_stats()
        total_searches = get_total_searches_count()
        contacts_df = fetch_contacts()
        total_contacts = len(contacts_df) if contacts_df is not None and not contacts_df.empty else 0
        recent_searches = get_recent_searches_count(7)
        avg_duration = get_average_trip_duration()
    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
        stats = {}
        total_searches = 0
        total_contacts = 0
        recent_searches = 0
        avg_duration = 0
    
    # Main KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Searches", stats.get('total_searches', total_searches))
    with col2:
        st.metric("Total Contacts", stats.get('total_contacts', total_contacts))
    with col3:
        st.metric("Last 7 Days", stats.get('searches_7d', recent_searches))
    with col4:
        st.metric("Last 24h", stats.get('searches_24h', 0))
    with col5:
        st.metric("Avg Trip Duration", f"{stats.get('avg_trip_duration', avg_duration):.1f} days")
    
    # Additional metrics
    col6, col7, col8, col9 = st.columns(4)
    
    with col6:
        try:
            budget_stats = get_budget_distribution()
            popular_budget = budget_stats[0][0] if budget_stats else "N/A"
        except:
            popular_budget = "N/A"
        st.metric("Popular Budget", popular_budget)
    
    with col7:
        try:
            class_stats = get_class_distribution()
            popular_class = class_stats[0][0] if class_stats else "N/A"
        except:
            popular_class = "N/A"
        st.metric("Popular Class", popular_class)
    
    with col8:
        try:
            monthly_searches = get_monthly_searches()
        except:
            monthly_searches = 0
        st.metric("This Month", monthly_searches)
    
    with col9:
        try:
            weekly_growth = get_weekly_growth_rate()
        except:
            weekly_growth = 0
        st.metric("Weekly Growth", f"{weekly_growth:+.1f}%")

def display_analytics_charts():
    """Display analytics charts"""
    st.markdown("## 📈 Analytics & Trends")
    
    # Get flight data
    flight_data = get_flight_analytics()
    
    if flight_data.empty:
        st.info("No flight data available for charts.")
        return
    
    # Top destinations and departures
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Top Destinations")
        try:
            df_destinations = get_top_destinations(10)
            if df_destinations is not None and not df_destinations.empty:
                fig = px.bar(df_destinations, x='count', y='destination', orientation='h',
                           title='Most Popular Destinations', color='count',
                           color_continuous_scale='viridis')
                fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No destination data available.")
        except Exception as e:
            st.error(f"Error loading destination data: {e}")
    
    with col2:
        st.markdown("### Top Departure Cities")
        try:
            df_departures = get_top_departures(10)
            if df_departures is not None and not df_departures.empty:
                fig = px.bar(df_departures, x='count', y='origin', orientation='h',
                           title='Most Popular Departure Cities', color='count',
                           color_continuous_scale='plasma')
                fig.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No departure data available.")
        except Exception as e:
            st.error(f"Error loading departure data: {e}")
    
    # Time-based analytics
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown("### Search Trends Over Time")
        try:
            df_time = get_searches_over_time()
            if df_time is not None and not df_time.empty:
                df_time['date'] = pd.to_datetime(df_time['date'])
                fig = px.line(df_time, x='date', y='count', title='Daily Search Volume', markers=True)
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No search trend data available.")
        except Exception as e:
            st.error(f"Error loading search trends: {e}")
    
    with col4:
        st.markdown("### Budget Distribution")
        if 'budget_preference' in flight_data.columns:
            budget_dist = flight_data['budget_preference'].value_counts()
            fig = px.pie(values=budget_dist.values, names=budget_dist.index, 
                       title='Budget Preference Distribution')
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

def display_customer_management():
    """Customer management section"""
    st.markdown("## 👥 Customer Management")
    
    customers = fetch_contacts(1000)
    
    if customers is None or customers.empty:
        st.info("No customer data available.")
        return
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_term = st.text_input("🔍 Search customers", placeholder="Name, email, or phone")
    with col2:
        show_count = st.selectbox("Show records", [25, 50, 100, "All"])
    with col3:
        sort_by = st.selectbox("Sort by", ["Registration Date", "Name", "Email"])
    
    # Apply filters
    filtered_customers = customers.copy()
    
    if search_term:
        mask = (
            customers['firstName'].str.contains(search_term, case=False, na=False) |
            customers['secondName'].str.contains(search_term, case=False, na=False) |
            customers['email'].str.contains(search_term, case=False, na=False) |
            customers['phone'].str.contains(search_term, case=False, na=False)
        )
        filtered_customers = customers[mask]
    
    # Limit results
    if show_count != "All":
        filtered_customers = filtered_customers.head(show_count)
    
    # Display
    st.markdown(f"### 📊 Customer Records ({len(filtered_customers)} shown)")
    
    if not filtered_customers.empty:
        display_data = filtered_customers.copy()
        
        # Format data for display
        if 'created_at' in display_data.columns:
            display_data['Registration Date'] = pd.to_datetime(
                display_data['created_at'], errors='coerce'
            ).dt.strftime('%Y-%m-%d %H:%M')
        
        column_mapping = {
            'firstName': 'First Name',
            'secondName': 'Last Name',
            'email': 'Email',
            'phone': 'Phone'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in display_data.columns:
                display_data[new_col] = display_data[old_col]
        
        display_columns = list(column_mapping.values())
        if 'Registration Date' in display_data.columns:
            display_columns.append('Registration Date')
        
        st.dataframe(display_data[display_columns], use_container_width=True)

def display_system_management():
    """System management section"""
    st.markdown("## 🔧 System Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 💾 Database Status")
        try:
            db_info = get_database_info()
            st.success("✅ Database: Connected")
            st.write(f"**File Size:** {db_info.get('file_size_mb', 'Unknown')} MB")
            
            for table, count in db_info.get('table_counts', {}).items():
                st.write(f"• {table}: {count:,} records")
        except Exception as e:
            st.error(f"❌ Database Error: {e}")
    
    with col2:
        st.markdown("### 📥 Data Export")
        
        if st.button("📊 Export Search Data"):
            try:
                search_data = fetch_all_searches()
                if search_data is not None and not search_data.empty:
                    csv = search_data.to_csv(index=False)
                    st.download_button(
                        "💾 Download Search Data",
                        csv,
                        f"flight_searches_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv"
                    )
                else:
                    st.warning("No search data available.")
            except Exception as e:
                st.error(f"Error: {e}")
        
        if st.button("👥 Export Contact Data"):
            try:
                contact_data = fetch_contacts()
                if contact_data is not None and not contact_data.empty:
                    csv = contact_data.to_csv(index=False)
                    st.download_button(
                        "💾 Download Contact Data",
                        csv,
                        f"contacts_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv"
                    )
                else:
                    st.warning("No contact data available.")
            except Exception as e:
                st.error(f"Error: {e}")

def main():
    """Main application function"""
    # Initialize database
    try:
        init_db()
        initialize_admin_system()
    except Exception as e:
        st.error(f"Database initialization error: {e}")
    
    # Check authentication
    if not admin_login():
        return
    
    # Sidebar navigation
    with st.sidebar:
        st.markdown("### 📋 Navigation")
        selected_section = st.radio(
            "Select Section:",
            [
                "📊 Overview & Metrics",
                "📈 Analytics & Charts",
                "👥 Customer Management",
                "✈️ Flight Management", 
                "🔧 System Management"
            ]
        )
    
    # Display selected section
    if selected_section == "📊 Overview & Metrics":
        display_overview_metrics()
    elif selected_section == "📈 Analytics & Charts":
        display_analytics_charts()
    elif selected_section == "👥 Customer Management":
        display_customer_management()
    elif selected_section == "✈️ Flight Management":
        # You can implement flight management display here
        st.markdown("## ✈️ Flight Management")
        flight_data = get_flight_analytics()
        if not flight_data.empty:
            st.dataframe(flight_data, use_container_width=True)
        else:
            st.info("No flight data available.")
    elif selected_section == "🔧 System Management":
        display_system_management()

if __name__ == "__main__":
    main()