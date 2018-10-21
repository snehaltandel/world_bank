import json
filepath = ''

with open(filepath) as f:
    response = json.load(f)
    created_at = response['created_at']
    user = response['user']
    user_id = user['id']
    user_name = user['name']
    user_location = user['location']
    follower_count = user['followers_count']
    retweet_count = response['retweet_count']
    tag = []
    entities = response['entities']['hashtags']
    for i in list(entities):
        j = i['text']
        tag.append(j)

    data_keys = ['created_at', 'id', 'name', 'location', 'followers_count', 'hashtags', 'retweet_count']
    data_values = [str(created_at), str(user_id), str(user_name), str(user_location), int(follower_count),
                   list(tag), int(retweet_count)]
    result = dict(zip(data_keys, data_values))
    with open('', 'a+') as f:
        json.dump(result, f)
