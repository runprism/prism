
TEMPLATE = """
{% raw %}{{
    conf(
        adapter_name="<adapter name here>",
        materialized="view"
    )
}}{% endraw %}

--SQL logic here!

"""
