def check_listing(listing:dict):

    schema = get_listing_schema()


    def matches(value, template):

        if isinstance(template, dict):

            return (

                isinstance(value, dict)

                and set(value) == set(template)

                and all(matches(value[key], template[key]) for key in template)

            )

        if isinstance(template, list):

            return isinstance(value, list) and all(

                matches(item, template[0]) for item in value

            ) if template else isinstance(value, list)

        return type(value) is type(template)


    return matches(listing, schema)


def get_listing_schema():
    """Get a listing schema, 
                    title, 
                    city, 
                    description, 
                    salary-month (if present or if possible to calculate reliably)
                    and salary-hour (if present or possible to calculate reliably)"""
    return {

        "title": "",
        "city": "",
        "description": "",
        "technologies": "",
        "salary-month": 0,
        "salary-hour": 0,
        "date": ""
    } 