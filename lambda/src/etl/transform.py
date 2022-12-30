def transform(users: list = []):
    users_by_country = {}
    for user in users:
        country = user['location']['country']
        if country not in users_by_country:
            users_by_country[country] = []
        users_by_country[country].append({
            'name': user['login']['username'],
            'gender': user['gender'],
            'email': user['email'],
        })
    result = []
    for k, v in users_by_country.items():
        result.append({ 'country': k, 'users': v })
    return result