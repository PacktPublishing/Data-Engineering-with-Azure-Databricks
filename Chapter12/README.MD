# Chapter 12: Machine Learning and AI on Databricks

## Summary

In this chapter, we will explore the AI and machine learning capabilities of Azure Databricks. We will start by understanding what makes Databricks unique for AI and ML workloads, examining how the unified platform approach accelerates the entire machine learning lifecycle from data preparation to production deployment. We will then dive into MLflow, the industry-standard framework for tracking experiments, managing model versions, and deploying models to production endpoints. Next, we will explore Feature Store and how Delta Live Tables enable centralized feature management that ensures consistency between training and serving. We will learn how AutoML can rapidly build production-quality models with minimal code, making machine learning accessible to analysts and data scientists alike. We will see how Genie enables business users to query data using natural language, democratizing data access across the organization. Finally, we will cover Mosaic AI and generative AI capabilities, including Vector Search for semantic document retrieval, building RAG applications that answer questions using your company's documents, and AI Gateway for governance and cost control of LLM usage. Throughout the chapter, we will focus on conceptual understanding with simple code examples and practical use case ideas across industries.

---

## Chapter Structure (6 Main Sections)

### 1. Introduction: Databricks AI/ML Innovation and Capabilities (4-5 pages)
**Goal**: Understand what makes Databricks unique for AI/ML and overview all capabilities

#### 1.1 The Databricks AI/ML Innovation
- The problem with traditional ML platforms
- Databricks' unified approach
- Key differentiators: open source, lakehouse, collaborative, scalable

#### 1.2 The Databricks AI/ML Stack
- Traditional ML path: Genie → AutoML → MLflow → Deployment
- Generative AI path: Genie → Agent Bricks → Mosaic AI → Deployment
- Shared foundation: Unity Catalog, MLflow, Model Serving

#### 1.3 ML and AI Capabilities Overview
- Traditional ML: MLflow, Feature Store, AutoML, Lakehouse Monitoring
- Generative AI: Genie, Agent Bricks, Mosaic AI, Vector Search, Foundation Models, AI Gateway

#### 1.4 Integration and Governance
- Unity Catalog as the governance layer
- ML lineage flow example
- GenAI lineage flow example

#### 1.5 Chapter Roadmap
- Overview of sections 2-6

#### Tutorials for Section 1

| Title | URL | Description |
|---|---|---|
| Tutorial: Build Your First ML Model on Databricks | https://docs.databricks.com/aws/en/getting-started/ml-get-started | Getting-started tutorial for building a classification model with MLflow tracking |
| AI and ML Tutorials Index | https://docs.databricks.com/aws/en/machine-learning/ai-ml-tutorials | Curated index of all quickstart notebooks and tutorials |
| Concepts: Generative AI on Databricks | https://docs.databricks.com/aws/en/generative-ai/guide/concepts/ | GenAI terminology, architecture patterns, and platform components |
| Introduction to Generative AI Apps on Databricks | https://docs.databricks.com/aws/en/generative-ai/guide/introduction-generative-ai-apps | Overview of building GenAI apps from RAG chatbots to tool-calling agents |
| MLOps Workflows on Databricks | https://docs.databricks.com/aws/en/machine-learning/mlops/mlops-workflow | End-to-end MLOps patterns with MLflow, Feature Store, and model serving |

---

### 2. MLflow: ML Experiment Tracking and Model Management (5-6 pages)
**Goal**: Understand MLflow for reproducible ML development and deployment

#### 2.1 What is MLflow?
#### 2.2 Core Concepts
#### 2.3 Simple Code Example: Tracking an Experiment
#### 2.4 Model Registry and Deployment
#### 2.5 Business Use Case Ideas

#### Tutorials for Section 2

**Documentation and Quickstarts**

| Title | URL | Description |
|---|---|---|
| MLflow on Databricks (Hub Page) | https://docs.databricks.com/aws/en/mlflow/ | Central landing page for all MLflow features on Databricks |
| Quickstart: Organize Training Runs with MLflow Experiments | https://docs.databricks.com/en/mlflow/quick-start.html | Create and manage MLflow experiments for training runs and evaluations |
| Track Model Development Using MLflow | https://docs.databricks.com/aws/en/mlflow/tracking | Log parameters, metrics, tags, and artifacts during model training |
| Tutorial: End-to-End Classic ML Models on Databricks | https://docs.databricks.com/aws/en/mlflow/end-to-end-example | Full notebook covering data loading, hyperparameter optimization, model registration, and inference |
| Log, Load, and Register MLflow Models | https://docs.databricks.com/aws/en/mlflow/models | Reference for logging and registering models using the MLflow API |
| Workspace Model Registry Example | https://docs.databricks.com/aws/en/mlflow/workspace-model-registry-example | Notebook walkthrough of model registration and lifecycle management |

**MLflow 3 for GenAI**

| Title | URL | Description |
|---|---|---|
| Get Started: MLflow 3 for GenAI | https://docs.databricks.com/aws/en/mlflow3/genai/getting-started/ | Quickstart for tracing, evaluation, and human feedback for GenAI apps |
| MLflow Tracing for GenAI Observability | https://docs.databricks.com/aws/en/mlflow3/genai/tracing | Record inputs, outputs, and intermediate steps for GenAI apps |
| 10-Minute Demo: Evaluate a GenAI App | https://docs.databricks.com/aws/en/mlflow3/genai/getting-started/eval | Evaluate GenAI applications using LLM judges and custom metrics |
| 10-Minute Demo: Collect Human Feedback | https://docs.databricks.com/aws/en/mlflow3/genai/getting-started/human-feedback | Collect end-user and expert feedback to improve GenAI app quality |
| Tutorial: Evaluate and Improve a GenAI Application | https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/evaluate-app | Iteratively improve GenAI app quality using MLflow 3 evaluation tools |
| Prompt Registry: Create and Edit Prompts | https://docs.databricks.com/aws/en/mlflow3/genai/prompt-version-mgmt/prompt-registry/create-and-edit-prompts | Manage prompt versions in the MLflow Prompt Registry |

**GitHub Repositories**

| Title | URL | Description |
|---|---|---|
| databricks/mlflow | https://github.com/databricks/mlflow | Official open-source MLflow repository |
| MLflow Example: Sklearn ElasticNet Wine | https://github.com/databricks/mlflow-example-sklearn-elasticnet-wine | Minimal end-to-end MLflow example using scikit-learn |
| databricks/databricks-ml-examples | https://github.com/databricks/databricks-ml-examples | Collection of fine-tuning and LLM inference notebooks |

---

### 3. Feature Store: Centralized Feature Management (5-6 pages)
**Goal**: Understand how to build reusable features that ensure consistency

#### 3.1 What is Feature Store?
#### 3.2 Core Concepts
#### 3.3 Simple Code Example: Creating Features
#### 3.4 Key Benefits
#### 3.5 Business Use Case Ideas

#### Tutorials for Section 3

**Documentation and Quickstarts**

| Title | URL | Description |
|---|---|---|
| Feature Engineering in Unity Catalog (Overview) | https://docs.databricks.com/aws/en/machine-learning/feature-store/ | Overview covering governance, lineage, and cross-workspace access |
| Feature Store Concepts and Glossary | https://docs.databricks.com/aws/en/machine-learning/feature-store/concepts | Key terms like feature tables, lookups, and online stores |
| Use Features to Train Models | https://docs.databricks.com/aws/en/machine-learning/feature-store/train-models-with-feature-store | Train models and perform batch inference using Feature Engineering |
| Tutorial: Deploy and Query a Feature Serving Endpoint | https://docs.databricks.com/aws/en/machine-learning/feature-store/feature-serving-tutorial | Low-latency feature calculations using the Online Feature Store |
| Feature Engineering Python API | https://docs.databricks.com/aws/en/machine-learning/feature-store/python-api | API reference for the Feature Engineering Python client |

**Demos and Blog**

| Title | URL | Description |
|---|---|---|
| Feature Store and Online Inference (Demo) | https://www.databricks.com/resources/demos/tutorials/data-science-and-ai/feature-store-and-online-inference | Full pipeline from feature ingestion through real-time online inference |
| What is a Feature Store? A Complete Guide | https://www.databricks.com/blog/what-feature-store-complete-guide-ml-feature-engineering | Blog post explaining the concept and its integration with MLflow |

---

### 4. AutoML: Rapid Model Development (4-5 pages)
**Goal**: Build production-quality models with minimal code

#### 4.1 What is AutoML?
#### 4.2 Core Concepts
#### 4.3 Simple Code Examples
#### 4.4 Using AutoML Results
#### 4.5 Business Use Case Ideas

#### Tutorials for Section 4

**Documentation and Quickstarts**

| Title | URL | Description |
|---|---|---|
| What is AutoML? (Overview) | https://docs.databricks.com/aws/en/machine-learning/automl/ | How Databricks AutoML finds the best algorithm and hyperparameters |
| Classification with AutoML | https://docs.databricks.com/aws/en/machine-learning/automl/classification | Set up classification using the AutoML UI or Python API |
| Regression with AutoML | https://docs.databricks.com/aws/en/machine-learning/automl/regression | Run AutoML regression experiments with the UI and Python API |
| Forecasting with AutoML | https://docs.databricks.com/aws/en/machine-learning/automl/forecasting | Find the best forecasting algorithm for time-series data |
| Train Classification Models with AutoML Python API | https://docs.databricks.com/en/machine-learning/automl/classification-train-api.html | Run AutoML classification programmatically in a notebook |
| Train Regression Models with AutoML Python API | https://docs.databricks.com/aws/en/machine-learning/automl/regression-train-api | Run AutoML regression via the Python API |
| AutoML Python API Reference | https://docs.databricks.com/aws/en/machine-learning/automl/automl-api-reference | Full API reference for all AutoML methods and parameters |
| AutoML Feature Store Integration | https://docs.databricks.com/gcp/en/machine-learning/automl/feature-store-integration | Augment AutoML datasets with features from Unity Catalog |
| AutoML Usage Example Notebook | https://docs.databricks.com/aws/en/notebooks/source/machine-learning/automl-usage-example.html | Runnable example notebook for end-to-end AutoML usage |

**GitHub**

| Title | URL | Description |
|---|---|---|
| databricks/automl | https://github.com/databricks/automl | Official Databricks AutoML repository |

---

### 5. Genie: Conversational Analytics for Business Users (3-4 pages)
**Goal**: Enable business users to get insights without writing SQL

#### 5.1 What is Genie?
#### 5.2 How Genie Works
#### 5.3 Setting Up Genie
#### 5.4 Example Interactions
#### 5.5 Business Use Case Ideas

#### Tutorials for Section 5

| Title | URL | Description |
|---|---|---|
| What is a Genie Space? (Overview) | https://docs.databricks.com/aws/en/genie/ | Natural language chat interface for exploring business data |
| Use a Genie Space to Explore Business Data | https://docs.databricks.com/aws/en/genie/talk-to-genie | Ask natural language questions and receive query results |
| Set Up and Manage a Genie Space | https://docs.databricks.com/aws/en/genie/set-up | Create and manage Genie spaces with datasets and guidelines |
| Curate an Effective Genie Space (Best Practices) | https://docs.databricks.com/aws/en/genie/best-practices | Bridge general knowledge and domain-specific language |
| Use the Genie API to Integrate into Applications | https://docs.databricks.com/aws/en/genie/conversation-api | Embed Genie into custom applications via REST API |
| Add a Genie Space to a Databricks App | https://docs.databricks.com/aws/en/dev-tools/databricks-apps/genie | Integrate a Genie space as a resource within a Databricks App |
| AI/BI Concepts | https://docs.databricks.com/aws/en/ai-bi/concepts | Foundational concepts behind AI/BI dashboards and Genie |

---

### 6. Generative AI: Building AI Applications with Databricks (10-12 pages)
**Goal**: Understand how to build AI agents and RAG applications

#### 6.1 Mosaic AI Platform Overview
#### 6.2 Vector Search for RAG
#### 6.3 Building RAG Applications
#### 6.4 AI Gateway: Governance and Cost Control
#### 6.5 Deployment and Monitoring
#### 6.6 Complete Use Case Example: Customer Support Chatbot

#### Tutorials for Section 6

**6.1 — Mosaic AI and Model Serving**

| Title | URL | Description |
|---|---|---|
| Mosaic AI Model Serving (Overview) | https://docs.databricks.com/aws/en/machine-learning/model-serving/ | Hub page for real-time and batch inference for custom and foundation models |
| Tutorial: Deploy and Query a Custom Model | https://docs.databricks.com/aws/en/machine-learning/model-serving/model-serving-intro | Create a model serving endpoint and send scoring requests |
| Create Custom Model Serving Endpoints | https://docs.databricks.com/aws/en/machine-learning/model-serving/create-manage-serving-endpoints | Create and manage endpoints via the UI or REST API |
| Create Foundation Model Serving Endpoints | https://docs.databricks.com/aws/en/machine-learning/model-serving/create-foundation-model-endpoints | Deploy pay-per-token or provisioned throughput foundation models |
| External Models in Mosaic AI Model Serving | https://docs.databricks.com/aws/en/generative-ai/external-models | Expose external LLM providers (OpenAI, Anthropic, etc.) as endpoints |
| Query Serving Endpoints for Custom Models | https://docs.databricks.com/aws/en/machine-learning/model-serving/score-custom-model-endpoints | Send inference requests via REST API and Python SDK |

**6.1 — Agent Framework**

| Title | URL | Description |
|---|---|---|
| Tutorial: Build, Evaluate, and Deploy a Retrieval Agent | https://docs.databricks.com/aws/en/generative-ai/tutorials/agent-framework-notebook | Build, evaluate, and deploy a RAG agent using Mosaic AI Agent Framework |
| Create an AI Agent | https://docs.databricks.com/aws/en/generative-ai/agent-framework/create-agent | Build AI agents using Mosaic AI Agent Framework with LangGraph examples |
| AI Agent Tools | https://docs.databricks.com/aws/en/generative-ai/agent-framework/agent-tool | Configure and attach tools (functions, vector search, SQL) to agents |
| Agent System Design Patterns | https://docs.databricks.com/aws/en/generative-ai/guide/agent-system-design-patterns | Common design patterns for building and scaling agentic systems |
| Agent Evaluation Quickstart | https://docs.databricks.com/aws/en/generative-ai/agent-evaluation/evaluation-quickstart | Evaluate AI agents using MLflow evaluation with LLM judges |
| Evaluate and Monitor AI Agents | https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/ | Monitor and improve agent quality using MLflow 3 |
| Use Agent Bricks with a Custom LLM | https://docs.databricks.com/aws/en/generative-ai/agent-bricks/custom-llm | Automate data-driven agent customization with Agent Bricks |
| Agentic Systems: Deploy and Evaluate RAG Apps (Demo) | https://www.databricks.com/resources/demos/tutorials/data-science/ai-agent | Build and deploy AI agents using LLMs, RAG, Vector Search, and MLflow |

**6.2 — Vector Search**

| Title | URL | Description |
|---|---|---|
| Mosaic AI Vector Search (Overview) | https://docs.databricks.com/aws/en/vector-search/vector-search | Delta Sync Indexes, Direct Vector Access Indexes, and hybrid search |
| Create Vector Search Endpoints and Indexes | https://docs.databricks.com/aws/en/vector-search/create-vector-search | Create endpoints and indexes using UI, Python SDK, or REST API |
| How to Create and Query a Vector Search Index | https://docs.databricks.com/gcp/en/generative-ai/create-query-vector-search | Combined index creation and querying tutorial |
| Query a Vector Search Index | https://docs.databricks.com/aws/en/vector-search/query-vector-search | Query vector indexes using Python SDK, REST API, and SQL |
| Vector Search Performance Guide | https://docs.databricks.com/aws/en/vector-search/vector-search-best-practices | Optimize vector search index performance and query latency |

**6.3 — RAG (Retrieval-Augmented Generation)**

| Title | URL | Description |
|---|---|---|
| RAG on Databricks (Overview) | https://docs.databricks.com/aws/en/generative-ai/retrieval-augmented-generation | End-to-end overview of building RAG apps with Vector Search and Delta Lake |
| Build an Unstructured Data Pipeline for RAG | https://docs.databricks.com/aws/en/generative-ai/tutorials/ai-cookbook/quality-data-pipeline-rag | Convert unstructured documents (PDFs, text) into a vector index |
| RAG Chain for Inference | https://docs.databricks.com/aws/en/generative-ai/tutorials/ai-cookbook/fundamentals-inference-chain-rag | Build the retrieval and generation chain for a RAG application |
| Improve RAG Chain Quality | https://docs.databricks.com/en/generative-ai/tutorials/ai-cookbook/quality-rag-chain.html | Iterate on and improve RAG chain quality using evaluation metrics |
| Introduction to RAG (AI Cookbook) | https://docs.databricks.com/en/ai-cookbook/rag-overview.html | Conceptual introduction to RAG in the Databricks AI workflow |
| Navigate the GenAI Agent Tutorial (Cookbook Index) | https://docs.databricks.com/en/generative-ai/tutorials/ai-cookbook/index.html | Index page for the GenAI Cookbook tutorial series |
| Connect Agents to Unstructured Data | https://docs.databricks.com/en/generative-ai/agent-framework/agent-retriever.html | Attach vector search retrievers to AI agents |
| LLM Chatbot with RAG and DBRX (Demo) | https://notebooks.databricks.com/demos/llm-rag-chatbot/index.html | End-to-end demo for building a RAG-powered LLM chatbot |
| Deploy LLM Chatbots with RAG (Product Tour) | https://www.databricks.com/resources/demos/tours/deploy-llm-chatbots-rag-and-databricks-ai-vector-search | Interactive product tour for deploying a RAG chatbot |

**6.4 — AI Gateway**

| Title | URL | Description |
|---|---|---|
| AI Gateway (Overview) | https://docs.databricks.com/aws/en/ai-gateway/ | Enterprise control plane for governing LLM endpoints |
| AI Gateway for Serving Endpoints | https://docs.databricks.com/aws/en/ai-gateway/overview-serving-endpoints | Govern and monitor access to generative AI models at serving endpoints |
| Configure AI Gateway on Model Serving Endpoints | https://docs.databricks.com/aws/en/ai-gateway/configure-ai-gateway-endpoints | Enable AI Gateway features on a model serving endpoint via UI or API |

**LLM Fine-Tuning (Advanced)**

| Title | URL | Description |
|---|---|---|
| Foundation Model Fine-Tuning (Overview) | https://docs.databricks.com/aws/en/large-language-models/foundation-model-training/ | Hub page for instruction fine-tuning and continued pre-training |
| Tutorial: Create and Deploy a Fine-Tuning Run | https://docs.databricks.com/aws/en/large-language-models/foundation-model-training/fine-tune-run-tutorial | Full tutorial for creating, submitting, and deploying a fine-tuning run |
| Fine-Tuning Using the UI | https://docs.databricks.com/en/large-language-models/foundation-model-training/ui.html | Set up a fine-tuning run without writing code |
| Fine-Tune Llama 3.2 1B with LoRA (Serverless GPU) | https://docs.databricks.com/aws/en/machine-learning/ai-runtime/examples/tutorials/sgc-sft-trl-deepspeed-llama-1b | LoRA fine-tuning using TRL and DeepSpeed ZeRO Stage 3 |
| Fine-Tune Olmo3 7B with Axolotl (QLoRA, Multi-GPU) | https://docs.databricks.com/aws/en/machine-learning/ai-runtime/examples/tutorials/sgc-olmo3-7b-lora-axolotl | QLoRA fine-tuning using Axolotl on multi-GPU serverless compute |
| LoRA Fine-Tuning of Qwen2-0.5B | https://docs.databricks.com/aws/en/machine-learning/ai-runtime/examples/tutorials/sgc-finetune-qwen2-0.5b | Parameter-efficient LoRA fine-tuning on serverless GPU |
| Distributed Fine-Tuning of Qwen2-0.5B with LoRA | https://docs.databricks.com/aws/en/machine-learning/ai-runtime/examples/tutorials/sgc-distributed-finetune-qwen2-0.5b | Distributed multi-GPU LoRA fine-tuning with Liger Kernels |
| Fine-Tune Llama-3.2-3B with Unsloth | https://docs.databricks.com/aws/en/machine-learning/ai-runtime/examples/tutorials/sgc-finetune-llama-unsloth | Fine-tuning using Unsloth's optimized LoRA implementation |
| Fine-Tune Llama 3.1 8B Using Mosaic LLM Foundry | https://docs.databricks.com/aws/en/machine-learning/ai-runtime/examples/tutorials/sgc-llama3-8b-llmfoundry | Fine-tuning using Mosaic LLM Foundry on Serverless GPU |
| GPU Examples Hub (LLMs) | https://docs.databricks.com/aws/en/machine-learning/sgc-examples/gpu-llms | Hub page for all LLM fine-tuning and inference notebook examples |

**GenAI Workflows and Evaluation**

| Title | URL | Description |
|---|---|---|
| GenAI App Developer Workflow | https://docs.databricks.com/gcp/en/generative-ai/tutorials/ai-cookbook/genai-developer-workflow | Recommended workflow from proof-of-concept to production |
| Define Quality: Evaluation Sets | https://docs.databricks.com/aws/en/generative-ai/tutorials/ai-cookbook/evaluate-define-quality | Create evaluation datasets for GenAI apps |
| Assess Performance: Metrics That Matter | https://docs.databricks.com/aws/en/generative-ai/tutorials/ai-cookbook/evaluate-assess-performance | Retrieval and response quality metrics for RAG evaluation |

---

## Hands-on Exercises

1. **`01_mlflow_experiment_tracking.py`** — This exercise demonstrates MLflow experiment tracking fundamentals. It covers creating experiments, logging parameters and metrics during model training, saving trained models as artifacts, and viewing results in the MLflow UI. The example uses a simple RandomForest classifier for customer churn prediction.

2. **`02_feature_store_with_dlt.py`** — This exercise shows how to create a feature table using Delta Live Tables and the Feature Engineering Client. It covers defining feature computation logic, creating training sets with automatic feature lookups, and using features during inference with automatic feature serving.

3. **`03_automl_forecasting.py`** — This exercise demonstrates using Databricks AutoML for time series forecasting. It covers running AutoML on sales data with multiple stores, interpreting trial results, accessing the best model and generated notebook, and registering the model to Unity Catalog for deployment.

4. **`04_vector_search_rag.py`** — This exercise shows how to build a RAG (Retrieval-Augmented Generation) application. It covers creating a vector search index on documents, performing semantic similarity searches, building a simple RAG function that combines search with LLM generation, and evaluating RAG quality with MLflow.

5. **`05_ai_gateway_setup.py`** — This exercise demonstrates setting up AI Gateway for LLM governance. It covers creating gateway endpoints that route to external LLMs, configuring rate limits per user, making requests through the gateway, and querying usage logs for cost analysis.

---

## Databricks Academy Courses

### Machine Learning Specialization

| Title | URL | Description |
|---|---|---|
| Get Started with Databricks for Machine Learning | https://www.databricks.com/training/catalog/get-started-with-databricks-for-machine-learning-2461 | Foundational course for ML workflows, feature engineering, and MLflow |
| Machine Learning with Databricks | https://www.databricks.com/training/catalog/machine-learning-with-databricks-2422 | Core ML course: model training, hyperparameter tuning, full ML lifecycle |
| Advanced Machine Learning with Databricks | https://www.databricks.com/training/catalog/advanced-machine-learning-with-databricks-3486 | Distributed training, complex pipelines, and production ML patterns |
| Machine Learning Model Development | https://www.databricks.com/training/catalog/machine-learning-model-development-2393 | Model development workflows with MLflow and Feature Store |
| Data Preparation for Machine Learning | https://www.databricks.com/training/catalog/data-preparation-for-machine-learning-2343 | Data exploration, cleaning, feature engineering, and feature storage |
| Feature Engineering at Scale | https://www.databricks.com/training/catalog/feature-engineering-at-scale-4706 | Scalable feature pipelines using Spark, Lakeflow, and Feature Store |
| Machine Learning Operations (MLOps) | https://www.databricks.com/training/catalog/machine-learning-operations-2401 | MLOps fundamentals, model lifecycle, and operationalizing ML models |
| Advanced Machine Learning Operations | https://www.databricks.com/training/catalog/advanced-machine-learning-operations-3508 | Testing strategies, environment management, and automated retraining |

### Generative AI Specialization

| Title | URL | Description |
|---|---|---|
| Get Started with Databricks for Generative AI | https://www.databricks.com/training/catalog/get-started-with-databricks-for-generative-ai-3480 | Practical intro to Mosaic AI, Vector Search, Agent Framework, and MLflow |
| Generative AI Fundamentals | https://www.databricks.com/training/catalog/generative-ai-fundamentals-1765 | No-prerequisite introduction to generative AI and LLMs |
| Generative AI Fundamentals Accreditation | https://www.databricks.com/training/catalog/generative-ai-fundamentals-accreditation-1811 | Accreditation for demonstrating foundational GenAI knowledge |
| Generative AI Engineering with Databricks | https://www.databricks.com/training/catalog/generative-ai-engineering-with-databricks-1980 | Build production RAG applications and agents with MLflow and Agent Bricks |
| Generative AI Solution Development | https://www.databricks.com/training/catalog/generative-ai-solution-development-2662 | Design and implement end-to-end GenAI solutions on Databricks |

### Training Hub

| Title | URL | Description |
|---|---|---|
| Training Catalog (All Courses) | https://www.databricks.com/training/catalog | Complete listing of all Databricks Academy courses |
| Training and Certification Home | https://www.databricks.com/learn/training/home | Main hub for all learning programs |
| Get Started with Generative AI (On-Demand) | https://www.databricks.com/resources/learn/training/get-started-with-generative-ai | Free on-demand training for GenAI concepts and tools |

---

## Databricks Certifications

| Certification | URL | Description |
|---|---|---|
| Certification Hub | https://www.databricks.com/learn/training/certification | Overview of all available Databricks certifications |
| Machine Learning Associate | https://www.databricks.com/learn/certification/machine-learning-associate | AutoML, MLflow, feature engineering, model deployment (6+ months experience recommended) |
| Machine Learning Professional | https://www.databricks.com/learn/certification/machine-learning-professional | Advanced MLOps, distributed training, Feature Store, production model serving |
| Generative AI Engineer Associate | https://www.databricks.com/learn/certification/genai-engineer-associate | RAG, Vector Search, Model Serving, MLflow, Unity Catalog (90 min, ~56 questions) |

---

## Quick Reference: Starting Points by Topic

| Topic | Start Here |
|---|---|
| MLflow (Classic ML) | https://docs.databricks.com/aws/en/mlflow/ |
| MLflow 3 (GenAI) | https://docs.databricks.com/aws/en/mlflow3/genai/getting-started/ |
| AutoML | https://docs.databricks.com/aws/en/machine-learning/automl/ |
| Feature Store | https://docs.databricks.com/aws/en/machine-learning/feature-store/ |
| Genie / AI/BI | https://docs.databricks.com/aws/en/genie/ |
| Model Serving | https://docs.databricks.com/aws/en/machine-learning/model-serving/ |
| Agent Framework | https://docs.databricks.com/aws/en/generative-ai/tutorials/agent-framework-notebook |
| Vector Search | https://docs.databricks.com/aws/en/vector-search/vector-search |
| RAG Cookbook | https://docs.databricks.com/en/generative-ai/tutorials/ai-cookbook/index.html |
| AI Gateway | https://docs.databricks.com/aws/en/ai-gateway/ |
| LLM Fine-Tuning | https://docs.databricks.com/aws/en/large-language-models/foundation-model-training/ |
| All Courses | https://www.databricks.com/training/catalog |
| All Certifications | https://www.databricks.com/learn/training/certification |
