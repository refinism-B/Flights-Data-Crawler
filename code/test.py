def gmap_info(ori_name, api_key, place_id):
    """提供place_id，回傳名稱、營業狀態、營業時間、gmap評分、經緯度、gmap網址、最新評論日期"""
    if pd.notna(place_id) and place_id not in (None, "", "nan"):
        try:
            gmaps = googlemaps.Client(key=api_key)
            detail = gmaps.place(place_id=place_id, language="zh-TW")
        except Exception as e:
            # API 呼叫失敗，回傳 minimal fallback
            return {
                "name": ori_name,
                "place_id": place_id,
                "business_status": None,
                "address": None,
                "phone": None,
                "opening_hours": None,
                "rating": None,
                "rating_total": None,
                "longitude": None,
                "latitude": None,
                "map_url": None,
                "newest_review": None,
            }

        result = detail.get("result") or {}
        name = result.get("name")
        business_status = result.get("business_status")

        formatted_address = result.get("formatted_address")
        adr_address = result.get("adr_address")
        if formatted_address:
            address = formatted_address
        elif adr_address:
            address = BeautifulSoup(adr_address, "html.parser").text
        else:
            address = None

        phone = result.get("formatted_phone_number")
        if isinstance(phone, str):
            phone = phone.replace(" ", "")

        opening_hours = result.get("opening_hours", {}).get("weekday_text")
        rating = result.get("rating")
        rating_total = result.get("user_ratings_total")
        longitude = result.get("geometry", {}).get("location", {}).get("lng")
        latitude = result.get("geometry", {}).get("location", {}).get("lat")
        map_url = result.get("url")
        review_list = result.get("reviews")
        newest_review = gm.newest_review_date(review_list) if review_list else None

        place_info = {
            "name": name,
            "place_id": place_id,
            "business_status": business_status,
            "address": address,
            "phone": phone,
            "opening_hours": opening_hours,
            "rating": rating,
            "rating_total": rating_total,
            "longitude": longitude,
            "latitude": latitude,
            "map_url": map_url,
            "newest_review": newest_review,
        }
    else:
        place_info = {
            "name": ori_name,
            "place_id": None,
            "business_status": None,
            "address": None,
            "phone": None,
            "opening_hours": None,
            "rating": None,
            "rating_total": None,
            "longitude": None,
            "latitude": None,
            "map_url": None,
            "newest_review": None,
        }

    return place_info