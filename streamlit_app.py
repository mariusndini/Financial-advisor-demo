import streamlit as st
import random
import time
import json
import pandas as pd

from snowflake.core import Root
from snowflake.snowpark.context import get_active_session

from snowflake.cortex import Complete
session = get_active_session()
root = Root(session)


SPLIT_TOKENS = 128000  # Max number of tokens to send to summary LLM Call (32768)
AI_MODEL = 'mistral-large'

SEARCH_TERM = 'artificial intelligence'

if "SEARCH_TERM" not in st.session_state:
    st.session_state.SEARCH_TERM = 'artifical intelligence'
    
# ##################################################################################################################
def cortex_Search(term, year):
    response = (
        root.databases['EARNINGS']
        .schemas['PUBLIC']
        .cortex_search_services['TRANSCRIPT_SERVICE']
        .search(
            query=term,
            columns =['transcript_chunk', 'fiscal_quarter', 'fiscal_year', 'stock_symbol'],
            filter = { "@eq": { "fiscal_year": f"{year}" } },
            limit = 10
        )
    )
    return json.loads(response.to_json())['results']




# ##################################################################################################################
def earnings_demo():
    # Custom CSS styling
    st.markdown("""
    <style>
        body {
            font-family: Arial, sans-serif;
        }
        .container {
            margin: 20px;
        }
        .data-item {
            display: flex;
            align-items: flex-start;
            margin-bottom: 15px;
            padding: 10px;
            border-radius: 0px;
            text-decoration: none;
            color: inherit;
        }
        .data-item img {
            margin-right: 10px;
            max-width: 100px;
            max-height: 100px;
        }
        .data-content {
            flex-grow: 1;
        }
        .data-content p {
            margin: 0px 0;
        }
        .query-date-section {
            margin-bottom: 0px;
        }
        .query-date-section h2 {
            border-bottom: 0px solid #ddd;
            padding-bottom: 0px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    # App title
    st.title("Cortex Graded Earnings")
    
    # Session setup
    session = get_active_session()
    
    SQL_ADD = ''
    ticker_in, sectors_op, industry_op, query_date, grade_op, country_op, dividend = None, None, None, None, None, None, [0,0]
    
    def make_sidebar(filters):
    # Sidebar inputs
        with st.sidebar:
            st.write(filters)
            ticker_in = st.text_input("Ticker")
        
            sectors_dropdown = session.sql(
                f"SELECT DISTINCT sector FROM EARNINGS.ANALYSIS.earnings_summaries_mv WHERE sector IS NOT NULL AND sector != '' {filters}"
            ).to_pandas()
            sectors_op = st.selectbox("Sector", sectors_dropdown['SECTOR'], index=None, placeholder="Select Sector")
        
            ind_sql = f"""
                SELECT DISTINCT industry FROM EARNINGS.ANALYSIS.earnings_summaries_mv
                WHERE industry IS NOT NULL AND industry != '' 
                {'AND sector = ' + f"'{sectors_op}'" if sectors_op else ''}
            """
            industry_dropdown = session.sql(ind_sql).to_pandas()
            industry_op = st.selectbox("Industry", industry_dropdown['INDUSTRY'], index=None, placeholder="Select Industry")
        
            query_date = st.date_input("Release Date", value=None)
        
            col1, col2 = st.columns(2)
            grade_op = col1.selectbox("Grade", ('A', 'B', 'C', 'D', 'F'), index=None, placeholder="Select Grade")
            
            country = session.sql(
                "SELECT DISTINCT country FROM EARNINGS.ANALYSIS.earnings_summaries_mv"
            ).to_pandas()
            country_op = col2.selectbox("Country", country['COUNTRY'], index=None, placeholder="Select Country")
        
            div_sql = "SELECT MIN(last_div) AS MN, MAX(last_div) AS MX FROM EARNINGS.ANALYSIS.earnings_summaries_mv"
            div_dropdown = session.sql(div_sql).to_pandas()
            dividend = st.slider("Dividend", div_dropdown['MN'][0], div_dropdown['MX'][0], (div_dropdown['MN'][0], div_dropdown['MX'][0]))
        return ticker_in, sectors_op, industry_op, query_date, grade_op, country_op, dividend
        # end make sidebar function - return all the values to be used by the rest of the app::
        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
    
    ticker_in, sectors_op, industry_op, query_date, grade_op, country_op, dividend = make_sidebar(SQL_ADD)
    
    
    # SQL query construction
    if ticker_in:
        SQL_ADD += f" AND UPPER(SYMBOL) = UPPER('{ticker_in}')"
    if sectors_op:
        SQL_ADD += f" AND sector = '{sectors_op}'"
    if industry_op:
        SQL_ADD += f" AND industry = '{industry_op}'"
    if grade_op:
        SQL_ADD += f" AND grade like '{grade_op}%'"
    if query_date:
        SQL_ADD += f" AND query_date = '{query_date}'"
    if country_op:
        SQL_ADD += f" AND country = '{country_op}'"
    
    
    SQL = f"""
    SELECT stock_symbol AS symbol,
           fiscal_quarter AS q,
           fiscal_year AS y,
           query_date,
           title,
           intro,
           analysis,
           positives,
           negatives,
           conclusion,
           grade,
           reason,
           company_name,
           industry,
           sector,
           market_cap,
           image,
           last_div,
           country,
           beta,
           created_at
    FROM earnings.analysis.earnings_summaries_mv
    WHERE TRUE {SQL_ADD}
    AND last_div BETWEEN {dividend[0]} AND {dividend[1]}
    ORDER BY query_date DESC, created_at DESC
    """
    
    # Fetching and displaying data
    session_data = session.sql(SQL).to_pandas()
    default_header_date = ''
    
    col1, col2, b8 = st.columns(3)
    col3, col4, col5, col6, col7 = st.columns(5)
    
    if len(session_data['SYMBOL']) ==0:
        st.header("No Data Available for the Selected Filters")
        st.subheader("Consider broadening your filter criteria")
    
    else:
        
        col1.metric(label="AI Evaluations ", value=len(session_data['SYMBOL']))
        
        # Value formatting function
        def format_value(value):
            if value >= 1_000_000_000_000:
                return f"{round(value / 1_000_000_000_000)}T"
            elif value >= 1_000_000_000:
                return f"{round(value / 1_000_000_000)}B"
            elif value >= 1_000_000:
                return f"{round(value / 1_000_000)}M"
            elif value >= 1_000:
                return f"{round(value / 1_000)}K"
            else:
                return f"{round(value)}"
        
        min_value = format_value(min(session_data['MARKET_CAP']))
        max_value = format_value(max(session_data['MARKET_CAP']))
        
        col2.metric(label="Market Cap Range", value=f"{min_value} - {max_value}")
        
        filtered_beta = session_data['BETA'][session_data['BETA'] != -392096.97].round(1)
        b8.metric(label="Beta Range", value=f"{max(min(filtered_beta),-50)} - {max(filtered_beta)}")
        
        grade_counts = session_data['GRADE'].value_counts()
        col3.metric(label="A Grades", value=int(grade_counts.get('A', 0) + grade_counts.get('A-', 0)))
        col4.metric(label="B Grades", value=int(grade_counts.get('B', 0) + grade_counts.get('B+', 0) + grade_counts.get('B-', 0)))
        col5.metric(label="C Grades", value=int(grade_counts.get('C', 0) + grade_counts.get('C+', 0) + grade_counts.get('C-', 0)))
        col6.metric(label="D Grades", value=int(grade_counts.get('D', 0) + grade_counts.get('D+', 0) + grade_counts.get('D-', 0)))
        col7.metric(label="F Grades", value=int(grade_counts.get('F', 0)))
        
        
        # Displaying earnings summaries
        for index, row in session_data.iterrows():
            if index > 50:
                break
        
            this_date = row['QUERY_DATE']
            if default_header_date != this_date:
                st.divider()
                st.header(this_date)
                default_header_date = this_date
                    
            try:
                img = row['IMAGE']
                year = row['Y']
                quarter = row['Q']
                symbol = row['SYMBOL']
        
                st.markdown(f"""
                    <div class='container'>
                        <div class='data-item'>
                            <img src='{img}' alt='GHG'>
                            <div class='data-content'>
                                <strong>{symbol}</strong> ({row.get('COMPANY_NAME', 'Unknown')})
                                <p><strong>Market Cap:</strong> ${format_value(row.get('MARKET_CAP', 0))} 
                                <strong>Sector:</strong>  {row.get('SECTOR', 'Unknown')} 
                                <strong>Industry:</strong> {row.get('INDUSTRY', 'Unknown')}</p>
                                <p><strong>Last Div:</strong> {float(row['LAST_DIV']) if row.get('LAST_DIV') else 'N/A'}%  
                                <strong>Beta:</strong> {float(row['BETA']) if row.get('BETA') else 'N/A'}</p>
                                <p><strong>Year:</strong> {year} 
                                <strong>Quarter:</strong> {quarter}  
                                <strong>Country:</strong> {row.get('COUNTRY', 'Unknown')}</p>
                                <p><strong>Cortex Evaluation:</strong> {row.get('GRADE', 'N/A')}</p>
                                <p><strong>Cortex Insight:</strong> {row.get('REASON', 'N/A')}</p>
                            </div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        
                with st.expander(f"{row['TITLE']}"):
                    st.subheader("Intro")
                    st.write(row['INTRO'].replace("$", "\$"))
                    st.subheader("Analysis")
                    st.write(row['ANALYSIS'].replace("$", "\$"))
                    st.subheader("Positives")
                    st.write(row['POSITIVES'].replace("$", "\$"))
                    st.subheader("Negatives")
                    st.write(row['NEGATIVES'].replace("$", "\$"))
                    st.subheader("Conclusion")
                    st.write(row['CONCLUSION'].replace("$", "\$"))
        
                    col1, col2, c3,c4,c5 = st.columns(5)
        
                    if col1.button(":thumbsup:", key=f"up{symbol}"):
                        session.sql(f"INSERT INTO EARNINGS.PUBLIC.SUMMARY_SCORE VALUES('{symbol}', {quarter}, {year}, 'up')").collect()
                    if col2.button(":thumbsdown:", key=f"down{symbol}"):
                        session.sql(f"INSERT INTO EARNINGS.PUBLIC.SUMMARY_SCORE VALUES('{symbol}', {quarter}, {year}, 'down')").collect()
                
            except Exception as e:
                print("Error displaying data item:", e)
            
    

# ##################################################################################################################
def chatbot_demo():
    with st.sidebar:
        st.write( f'Topic: {st.session_state.SEARCH_TERM}' )
    
    # Streamed response emulator
    def response_generator():
        llm_text = str(st.session_state.messagesC).replace("'", '"')
        
        response = Complete(AI_MODEL, llm_text)
        
        for word in response.split():
            yield word + " "
            time.sleep(0.02)
    
    
    st.title("Earnings Chat")
    # st.write(st.session_state.SEARCH_TERM)
    
    # Initialize chat history
    if "messagesC" not in st.session_state:
        st.session_state.messagesC = []
        searchData = cortex_Search(st.session_state.SEARCH_TERM, 2024)
        st.session_state.messagesC.append({"role": "system", "content": 
    f"""    
-- TASK
You are a seasoned stock advisor tasked with providing insightful and actionable advice based on earnings transcript summaries. 
Respond to user inquiries with precision, focusing on key financial metrics and market implications. 

Ensure your responses are clear, concise, and formatted for optimal readability.

-- OUTPUT FORMAT
Your output must be formatted for a Streamlit Application.

-- MOTIVATION
The output needs to provide maximum user engagement and informed decision-making for stock trading.

    """})
        
        st.session_state.messagesC.append({"role": "system", "content": searchData })
        st.session_state.messagesC.append({"role": "assistant", "content": f"""Hi, I am a chatbot here to assist with your inqueries and I have been given context about companies earnings."""})
    
    # Display chat messages from history on app rerun
    for message in st.session_state.messagesC:
        if message["role"] != 'system': #hide all system messages from the user
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    
    # Accept user input
    if prompt := st.chat_input("How May I help you?"):
        # Add user message to chat history
        st.session_state.messagesC.append({"role": "user", "content": prompt})
        # Display user message in chat message container
        with st.chat_message("user"):
            st.markdown(prompt)
    
        # Display assistant response in chat message container
        with st.chat_message("assistant"):
            response = st.write_stream(response_generator())
        # Add assistant response to chat history
        st.session_state.messagesC.append({"role": "assistant", "content": response})
    
    content_str = str(st.session_state.messagesC).replace("'", '"')
    
    query = f"SELECT SNOWFLAKE.CORTEX.COUNT_TOKENS( '{AI_MODEL}', '{content_str}' ) as TOKEN_LEN;"
    token_len = session.sql(query).to_pandas().iloc[0]['TOKEN_LEN']
    
    st.write(f'{token_len} Tokens')
    





# ##################################################################################################################
def search_demo():
    st.header("Cortex Earnings Search")
    output = {}
    
    with st.sidebar:
        st.divider()
        search_topic = st.text_input("Search Term 👇",)
        search_year = st.text_input("Fiscal Year", value=2024)

        if search_topic:
            SEARCH_TERM = search_topic
            st.session_state.SEARCH_TERM = search_topic
            output = cortex_Search(search_topic, search_year)

    output = st.json(output, expanded=2)




# ##################################################################################################################
def Analyst_demo():
    import _snowflake
    import json
    import streamlit as st
    import time
    from snowflake.snowpark.context import get_active_session
    
    DATABASE = "EARNINGS"
    SCHEMA = "ANALYSIS"
    STAGE = "SEM_MODEL"
    FILE = "earning_reco.yaml"
    
    def send_message(prompt: str) -> dict:
        """Calls the REST API and returns the response."""
        request_body = {
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                }
            ],
            "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        }
        resp = _snowflake.send_snow_api_request(
            "POST",
            f"/api/v2/cortex/analyst/message",
            {},
            {},
            request_body,
            {},
            30000,
        )
        if resp["status"] < 400:
            return json.loads(resp["content"])
        else:
            raise Exception(
                f"Failed request with status {resp['status']}: {resp}"
            )
    
    def process_message(prompt: str) -> None:
        """Processes a message and adds the response to the chat."""
        st.session_state.messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Generating response..."):
                response = send_message(prompt=prompt)
                content = response["message"]["content"]
                display_content(content=content)
        st.session_state.messages.append({"role": "assistant", "content": content})
    
    
    def display_content(content: list, message_index: int = None) -> None:
        """Displays a content item for a message."""
        message_index = message_index or len(st.session_state.messages)
        for item in content:    
            if item.get("type") == "text":
                st.markdown(item["text"])
            elif item["type"] == "suggestions":
                with st.expander("Suggestions", expanded=True):
                    for suggestion_index, suggestion in enumerate(item["suggestions"]):
                        if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                            st.session_state.active_suggestion = suggestion
            elif item["type"] == "sql":
                with st.expander("SQL Query", expanded=False):
                    st.code(item["statement"], language="sql")
                with st.expander("Results", expanded=True):
                    with st.spinner("Running SQL..."):
                        session = get_active_session()
                        df = session.sql(item["statement"]).to_pandas()
                        if len(df.index) > 1:
                            data_tab, line_tab, bar_tab = st.tabs(
                                ["Data", "Line Chart", "Bar Chart"]
                            )
                            data_tab.dataframe(df)
                            if len(df.columns) > 1:
                                df = df.set_index(df.columns[0])
                            with line_tab:
                                st.line_chart(df)
                            with bar_tab:
                                st.bar_chart(df)
                        else:
                            st.dataframe(df)
    
    
    st.title("Financial Analysis")
    #st.markdown(f"Semantic Model: `{FILE}`")
    
        
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.suggestions = []
        st.session_state.active_suggestion = None
        
    
    for message_index, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            display_content(content=message["content"], message_index=message_index)
    
    if user_input := st.chat_input("What is your question?"):
        process_message(prompt=user_input)
    
    if st.session_state.active_suggestion:
        process_message(prompt=st.session_state.active_suggestion)
        st.session_state.active_suggestion = None



 



# ##################################################################################################################
page_names_to_funcs = {
    "☎️ Earnings Calls": earnings_demo,
    "🔎 Cortex Search": search_demo,
    "🕵️‍♂️ Search-Bot": chatbot_demo,
    "🤖 Cortex Analyst": Analyst_demo,
}

demo_name = st.sidebar.selectbox("Cortex Search Demo(s)", page_names_to_funcs.keys())
page_names_to_funcs[demo_name]()
