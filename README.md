# Oblivious Query Retrieval

This repository supports the "Keep Your Secrets" research paper written by JS & MR. 
The goal of the paper is to demonstrate a light-weight pattern that allows for LLM & data model interactivity without exposing the underlying data to the LLM.

## Installation

To run this application, you'll need to install the necessary dependencies:

- streamlit
- pandas
- duckdb
- datasketch

or `pip install -r requirements.txt`

## Running the Application

To start the application, navigate to your project directory in the terminal and run:

`streamlit run obscura.py`

## Features

### Database Loading

- Build Database File: Load CSV files from a specified directory into the DuckDB database.
- Please note that DuckDB is configured as a file database for persistence.

### SQL Query Execution

- Database Schema: View the schema of the currently loaded tables in the database.
- Write SQL Query: Input and execute custom SQL queries against the DuckDB database.
- The output of DML queries executed against the file DB will be persisted.

### Database Utilities

- Build Similarity Index: Generate a similarity index for data assets.
- Build Cardinality Index: Create a cardinality index for database tables.
- Build Relation Map: Establish a relation map based on the cardinality and similarity indexes.
- Serialize Relation Map: Filter invalid relations and render the relation map in a human readable format suitable for ingestion by an LLM.

## Usage

The application interface is divided into several sections:

1. **Database Schema:** View the current schema of the database. Reload the application to reflect any changes made to the database.

2. **Write SQL Query:** This section allows you to write and execute SQL queries directly. DML operations are supported.

3. **Utilities:** Buttons for utility functions: building database files, similarity indexes, cardinality indexes, and relation maps.

4. **Serialize Relation Map**: Render & download the relation map in a human readable format for interaction with an LLM. 

## License 

MIT License found in `LICENSE.TXT`

---

# Keep Your Secrets

## Oblivious Query Retrieval: Enhancing Business Intelligence through Large Language Models (LLMs) and Secure Data Model Interaction

### Abstract

The rapid advancement of relational data models forms the backbone of most contemporary organizations, necessitating substantial resources for their curation and maintenance. With business intelligence tools often restricted to fixed visualizations and limited user-input options, there is a pressing need for innovation. Large Language Models (LLMs) hold the promise of revolutionizing business intelligence through their capabilities in understanding unstructured queries, relational data model comprehension, and advanced code generation. Our hypothesis proposes that LLMs can interact with data models without exposing the underlying data, achieving this in a secure, efficient, and cost-effective manner. This paper introduces the Oblivious Query Retrieval pattern, a novel approach leveraging LLMs for enhanced business intelligence.

### Introduction

In the realm of contemporary organizations, relational data models are fundamental, serving as the cornerstone of data management and analysis. These models, however, require significant resources for their maintenance, especially to meet the evolving needs of organizational stakeholders. Current business intelligence tools, predominantly built around static visualizations and user-input widgets, are often limited in their adaptability and efficiency.

The emergence of Large Language Models (LLMs) presents a groundbreaking opportunity in this domain. LLMs are uniquely capable of understanding unstructured queries, comprehending serialized relational maps, inferring user intent, and generating code, including SQL. This capability opens new avenues for business intelligence, potentially transforming how organizations interact with their data.

### Hypothesis

Our hypothesis posits that Large Language Models (LLMs) can engage with relational data models without directly accessing the underlying data. This interaction is hypothesized to be secure, rapid, and cost-effective, while also offering end-users the freedom to choose their preferred LLM. The core of this hypothesis lies in the ability of LLMs to process and generate complex queries based on a deep yet indirect understanding of the data models, thereby maintaining data privacy and security.

### Methodology

#### Required Supplemental Relational Data Model Objects

##### Similarity Index

- **Similarity Calculation:** Implementation of the MinHash algorithm for computing the Jaccard similarity coefficient.
- **Index Creation:** Criteria for adding relations to the index based on the Jaccard similarity coefficient.

##### Cardinality Index

- **Calculation Method:** Determining the ratio of distinct to total non-null values in each column.
- **Usage:** How this index aids in understanding data distribution and diversity.

##### Relation Map

- **Composition:** Inclusion of similarity indexes, weight index, and priority considerations.
- **Priority Assignment:** Criteria for prioritizing pk = fk relationships and the role of the similarity index.

##### Serialized Relation Map

- **Format:** Conversion of the relation map into a human-readable format.
- **Contents:** Description of database schema and table relationships.

#### Process

- **Relation Map:** The relation map is programmatically instantiated.
- **Input Preparation:** The serialized relation map, coupled with an unstructured human text query, is prepared for LLM analysis.
- **LLM Interaction:** This input is sent to an LLM with instructions to formulate a SQL query.
- **Query Execution:** The generated SQL query is executed locally, ensuring that the data model is not exposed to the LLM.

### Results and Discussion

In this section, we would analyze the practical application of the Oblivious Query Retrieval pattern and discuss its implications. The effectiveness of this approach is evaluated based on criteria such as security, efficiency, and user flexibility.

#### Effectiveness in Security

- **Data Privacy**
   - Illustration of how the relational data is never directly exposed to the LLM, ensuring high levels of data privacy.
- **Secure Query Processing**
   - Discussion on the secure handling of SQL queries generated by the LLM.

#### Efficiency Analysis

- **Query Response Time**
   - Evaluation of the time taken for the LLM to process queries and for the system to execute them.
- **Resource Utilization**
   - Assessment of the computational resources required for this method.
- **User Flexibility and Experience**
   - **Choice of LLMs**
      - Analysis of how the system supports different LLMs, offering users flexibility.
   - **Ease of Use**
      - Evaluation of the user experience in terms of the simplicity and intuitiveness of interacting with the system.

_Note: Detailed results and data to support these points would be included here._

### Conclusion

The Oblivious Query Retrieval pattern demonstrates significant potential in revolutionizing business intelligence. By leveraging LLMs while ensuring data security and operational efficiency, this approach offers a novel pathway in data analysis and decision-making processes. The implications of this study extend to various sectors, indicating a substantial shift in how organizations can harness the power of their data models.

### Future Work

Suggestions for future research include:
- **Expanding LLM Capabilities:** Investigating the integration of more advanced LLMs and their impact on system performance.
- **Scalability Studies:** Conducting studies on the scalability of this model in larger and more complex data environments.
- **User-Centric Design Improvements:** Exploring ways to enhance the user interface and experience for non-technical users.

### References

- Placeholder for Reference 1
- Placeholder for Reference 2
- Placeholder for Reference 3
