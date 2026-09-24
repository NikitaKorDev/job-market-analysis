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

    return {

        "title": "",
        "city": "",
        "description": "",
        "salary-month": "",
        "salary-hour": ""

    } 