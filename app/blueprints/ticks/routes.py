from flask import Blueprint, request, jsonify, url_for
from flask_paginate import Pagination
from app.extensions import get_db
from .storage import table_name
from typing import Any, Literal, cast, Union, Tuple
from flask import Response

# Blueprint

ticks_bp = Blueprint("ticks_data", __name__)

PER_PAGE = 100

# --------------------------------------------------------------
# Validate market type
# --------------------------------------------------------------

def validate_market(market: str) -> Literal["binance_spot", "binance_futures"]:
    valid: tuple[Literal["binance_spot", "binance_futures"], ...] = (
        "binance_spot",
        "binance_futures",
    )
    if market not in valid:
        raise ValueError("Invalid market")
    return cast(Literal["binance_spot", "binance_futures"], market)


# --------------------------------------------------------------
# Route
# --------------------------------------------------------------

@ticks_bp.route("/<market>/<symbol>")
def get_ticks(market: str, symbol: str) -> Union[dict[str, Any], Tuple[Response, int]]:
    try:
        market_valid = validate_market(market)
    except ValueError:
        return jsonify(error="Invalid market. Use 'binance_spot' or 'binance_futures'"), 400

    symbol_upper = symbol.upper()
    table = table_name(market_valid, symbol_upper)

    # Check if table exists
    exists = (
        get_db()
        .execute("SELECT 1 FROM duckdb_tables() WHERE name = ?", [table])
        .fetchone()
        is not None
    )

    if not exists:
        return {
            "market": market_valid,
            "symbol": symbol_upper,
            "total_trades": 0,
            "page": 1,
            "per_page": PER_PAGE,
            "data": [],
            "pagination": {},
        }

    # Pagination and filters
    page = max(request.args.get("page", 1, type=int), 1)
    start_ts = request.args.get("start", type=int)
    end_ts = request.args.get("end", type=int)

    where_clauses: list[str] = []
    params: list[int] = []

    if start_ts is not None:
        where_clauses.append("event_time >= make_timestamp(?)")
        params.append(start_ts * 1000)
    if end_ts is not None:
        where_clauses.append("event_time <= make_timestamp(?)")
        params.append(end_ts * 1000)

    where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

    # Total count
    count_row = get_db().execute(
        f'SELECT COUNT(*) FROM "{table}" WHERE 1=1 {where_sql}', params
    ).fetchone()
    total: int = int(count_row[0]) if count_row and count_row[0] is not None else 0

    # Page data
    offset = (page - 1) * PER_PAGE
    query = f'''
        SELECT trade_id, price, qty, quote_qty, event_time, is_buyer_maker
        FROM "{table}"
        WHERE 1=1 {where_sql}
        ORDER BY event_time DESC
        LIMIT ? OFFSET ?
    '''
    df = get_db().execute(query, params + [PER_PAGE, offset]).df()

    # Pagination metadata
    pagination = Pagination(
        page=page,
        per_page=PER_PAGE,
        total=total,
        record_name="trades",
        css_framework="bootstrap5",
        show_single_page=False,
    )

    base_args: dict[str, Any] = {k: v for k, v in request.args.items(multi=True) if k != "page"}

    def make_url(page_num: int) -> str:
        return url_for(
            "ticks_data.get_ticks",
            market=market_valid,
            symbol=symbol_upper.lower(),
            page=page_num,
            **base_args,
        )

    pag_dict: dict[str, Any] = {
        "page": page,
        "per_page": PER_PAGE,
        "total": total,
        "pages": pagination.pages or 1,
        "has_prev": pagination.has_prev,
        "has_next": pagination.has_next,
    }

    if pagination.has_prev:
        pag_dict["prev"] = make_url(page - 1)
    if pagination.has_next:
        pag_dict["next"] = make_url(page + 1)

    pag_dict["first"] = make_url(1)
    pag_dict["last"] = make_url(pagination.pages or 1) # type: ignore

    return {
        "market": market_valid,
        "symbol": symbol_upper,
        "total_trades": total,
        "page": page,
        "per_page": PER_PAGE,
        "data": df.to_dict(orient="records"),
        "pagination": pag_dict,
    }
