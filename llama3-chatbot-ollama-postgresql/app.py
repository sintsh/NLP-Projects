from langchain_community.llms import Ollama
from langchain_core.prompts import ChatPromptTemplate
from flask import Flask, request, render_template
import logging
import re
import psycopg2
from psycopg2.extras import RealDictCursor  # For dict-like row access

# Setup basic logging
logging.basicConfig(level=logging.DEBUG)

# Create Flask app
app = Flask(__name__)

# Database setup: Connect to PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="your_database_name",
            user="userName",
            password="password",
            host="127.0.0.1",
            port="5432",
            options="-c search_path=schema_name"  # Ensure the dat schema is used
        )
        return conn
    except psycopg2.Error as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        raise



# Define a function to format text by converting Markdown bold syntax to HTML strong tags
def format_output(text):
    return re.sub(r'\*\*(.*?)\*\*', r'<strong>\1</strong>', text)


# Function to retrieve query results from PostgreSQL database
def read_sql_query(sql):
    conn = connect_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)  # Enable dictionary-like access to rows
        cur.execute(sql)
        rows = cur.fetchall()  # Fetch rows as dictionary-like objects
        conn.commit()
        cur.close()
        return rows
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL error: {e}")
        return None
    finally:
        conn.close()


# Define chatbot initialization
def initialise_llama3():
    try:
        sql_conversion_prompt = ChatPromptTemplate.from_messages(
            [
                ("system", "You are an expert in converting English questions into SQL queries."),
                ("user", "Question: {question}"),
                ("system", "Here are some examples:\n"
                           "1. Question: How many students are there in the database?\n"
                           "   text: SELECT COUNT(*) FROM STUDENT;\n\n"
                           "2. Question: List all students who are in the Data Science class.\n"
                           "   text: SELECT * FROM STUDENT WHERE CLASS = 'Data Science';\n\n"
                           "3. Question: What are the names of students in section A?\n"
                           "   text: SELECT NAME FROM STUDENT WHERE SECTION = 'A';\n\n"
                           "Use the same pattern to generate SQL queries from natural language questions.")
            ]
        )
        return sql_conversion_prompt
    except Exception as e:
        logging.error(f"Failed to initialize chatbot: {e}")
        raise


# Initialize SQL conversion prompt
sql_conversion_prompt = initialise_llama3()


# Function to generate SQL query using the Ollama model
# Function to generate SQL query using the Ollama model
def get_llama_response(question):
    llm = Ollama(model="llama3.1")
    
    prompt = f"""
    You are an expert in converting English questions into SQL queries. Given a question, return the corresponding SQL query.
    Here are some examples:

    1. Question: How many students are there in the database?
       SQL Query: SELECT COUNT(*) FROM STUDENT;

    2. Question: List all students who are in the Data Science class.
       SQL Query: SELECT * FROM STUDENT WHERE CLASS = 'Data Science';

    3. Question: What are the names of students in section A?
       SQL Query: SELECT NAME FROM STUDENT WHERE SECTION = 'A';

    4. Question: How many students are there in section B?
       SQL Query: SELECT COUNT(*) FROM STUDENT WHERE SECTION = 'B';

    5. Question: List all orders and their customer names.
       SQL Query: SELECT orders.order_id, customers.name FROM orders JOIN customers ON orders.customer_id = customers.customer_id;

    6. Question: What is the total sales amount by each product?
       SQL Query: SELECT product_name, SUM(sales_amount) FROM sales GROUP BY product_name;

    7. Question: Get the names of employees who worked on more than 3 projects.
       SQL Query: SELECT employee_name FROM employees JOIN projects ON employees.employee_id = projects.employee_id GROUP BY employee_name HAVING COUNT(projects.project_id) > 3;

    8. Question: Find the top 5 products by sales amount.
       SQL Query: SELECT product_name, SUM(sales_amount) AS total_sales FROM sales GROUP BY product_name ORDER BY total_sales DESC LIMIT 5;

    9. Question: What are the average grades of students grouped by class?
       SQL Query: SELECT class, AVG(grade) AS average_grade FROM student_grades GROUP BY class;

    10. Question: List customers who have placed more than 5 orders in the last month.
        SQL Query: SELECT customers.name FROM customers JOIN orders ON customers.customer_id = orders.customer_id 
                    WHERE orders.order_date > NOW() - INTERVAL '1 month' 
                    GROUP BY customers.customer_id HAVING COUNT(orders.order_id) > 5;

    11. Question: Get the names of employees along with their highest salary.
        SQL Query: SELECT e.name, MAX(s.salary) AS highest_salary 
                    FROM employees e JOIN salaries s ON e.employee_id = s.employee_id 
                    GROUP BY e.name;

    12. Question: Find the average salary of employees in each department.
        SQL Query: SELECT d.department_name, AVG(e.salary) AS average_salary 
                    FROM departments d JOIN employees e ON d.department_id = e.department_id 
                    GROUP BY d.department_name;

    13. Question: Get the ranking of products based on sales using window functions.
        SQL Query: SELECT product_name, SUM(sales_amount) AS total_sales,
                    RANK() OVER (ORDER BY SUM(sales_amount) DESC) AS sales_rank 
                    FROM sales GROUP BY product_name;

    Use the same pattern to generate SQL queries from natural language questions. Avoid using "```" or any extraneous formatting.
    """

    # Generate the SQL query using the model
    response = llm.generate([prompt + f"\nQuestion: {question}"])

    logging.debug(f"Full response: {response}")

    if response.generations and len(response.generations) > 0:
        generation_chunk = response.generations[0][0]  # First chunk in the first generation
        generated_output = generation_chunk.text  # Extract the SQL query

        logging.debug(f"Generated output: {generated_output}")
        
        return generated_output.strip()

    logging.error("No valid SQL query found in the generated output.")
    return None



# Function to execute SQL query in PostgreSQL
def execute_sql(query):
    try:
        results = read_sql_query(query)
        logging.debug(f"Executed SQL query: {query}, Results: {results}")
        return results
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL error: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return None


# Define route for home page
@app.route('/', methods=['GET', 'POST'])
def main():
    query_input = None
    output = None
    if request.method == 'POST':
        query_input = request.form.get('query-input')
        if query_input:
            try:
                sql_query = get_llama_response(query_input)
                logging.debug(f"Generated SQL query: {sql_query}")

                results = execute_sql(sql_query)
                
                if results and isinstance(results, list):
                    output = "<table border='1'><tr>" + "".join([f"<th>{col}</th>" for col in results[0].keys()]) + "</tr>"
                    for row in results:
                        output += "<tr>" + "".join([f"<td>{val}</td>" for val in row.values()]) + "</tr>"
                    output += "</table>"
                else:
                    output = "No results found or an error occurred."
                    
            except Exception as e:
                logging.error(f"Error during SQL generation or execution: {e}")
                output = "Sorry, an error occurred while processing your request."
    return render_template('index.html', query_input=query_input, output=output)

if __name__ == '__main__':
    app.run(debug=True)
