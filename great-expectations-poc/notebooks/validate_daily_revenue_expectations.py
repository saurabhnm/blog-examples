#!/usr/bin/env python
# coding: utf-8

# In[13]:


import json
import great_expectations as ge
import great_expectations.jupyter_ux
import pandas as pd
from datetime import datetime


# In[2]:


context = ge.data_context.DataContext()


# In[3]:


data_asset_name = "daily_revenue" # TODO: replace with your value!
normalized_data_asset_name = context.normalize_data_asset_name(data_asset_name)
normalized_data_asset_name


# In[4]:


expectation_suite_name = "failure" # TODO: replace with your value!


# In[5]:


context.get_expectation_suite(normalized_data_asset_name, expectation_suite_name)


# In[6]:


batch_kwargs = {'query': "SELECT * FROM daily_revenue WHERE load_date = '{}'".format('2019-12-02')}
batch = context.get_batch(normalized_data_asset_name, expectation_suite_name, batch_kwargs)
batch.head()


# In[10]:


run_id = datetime.utcnow().isoformat().replace(":", "") + "Z"
run_id


# In[21]:


validation_result = batch.validate(run_id=run_id)

if validation_result["success"]:
    print("This data meets all expectations for {}".format(str(data_asset_name)))
else:
    print("This data is not a valid batch of {}".format(str(data_asset_name)))


# In[15]:


print(json.dumps(validation_result, indent=4))


# In[22]:


if not validation_result["success"]:
    raise ValueError("This data is not a valid batch of {}".format(str(data_asset_name)))


# In[16]:


#context.open_data_docs()


# In[17]:


#context.build_data_docs()


# In[19]:


# results = context.run_validation_operator(
#     assets_to_validate=[batch],
#     run_id=run_id,
#     validation_operator_name="action_list_operator",
# )


# In[ ]:




