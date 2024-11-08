defmodule DBN do
  @moduledoc """
  Databento Binary Encoding.

  See: https://databento.com/docs/standards-and-conventions/databento-binary-encoding

  ## Metadata Version 1 Layout

        0                   1                   2                   3
        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |               magic string and version = "DBN\1"              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                            length                             |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                                                               |
      +                                                               +
      |                                                               |
      +                            dataset                            +
      |                                                               |
      +                                                               +
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |             schema            |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                       start (UNIX nanos)                      |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                        end (UNIX nanos)                       |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                      limit (max records)                      |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                            reserved                           |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |   stype_in    |   stype_out   |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |    ts_out     |                                               |
      +-+-+-+-+-+-+-+-+                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                 reserved (47 bytes of padding)                |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                    schema_definition_length                   |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                  schema_definition (variable)                 |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         symbols_count                         |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                       symbols (variable)                      |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         partial_count                         |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                       partial (variable)                      |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                        not_found_count                        |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                      not_found (variable)                     |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         mappings_count                        |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                      mappings (variable)                      |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-end metadata; begin body--+-+-+-+-+-+-+-+-+-+
      |                            records                            |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  ## Metadata Version 2 Layout

        0                   1                   2                   3
        0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |               magic string and version = "DBN\2"              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                            length                             |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                                                               |
      +                                                               +
      |                                                               |
      +                            dataset                            +
      |                                                               |
      +                                                               +
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |             schema            |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                       start (UNIX nanos)                      |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                        end (UNIX nanos)                       |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
      |                      limit (max records)                      |
      +                               +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                               |   stype_in    |   stype_out   |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |    ts_out     |        symbol_cstr_len        |               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+               |
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                 reserved (53 bytes of padding)                |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +                                                               +
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                    schema_definition_length                   |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                  schema_definition (variable)                 |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         symbols_count                         |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                       symbols (variable)                      |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         partial_count                         |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                       partial (variable)                      |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                        not_found_count                        |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                      not_found (variable)                     |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         mappings_count                        |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                      mappings (variable)                      |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-end metadata; begin body--+-+-+-+-+-+-+-+-+-+
      |                            records                            |
      |                              ...                              |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

  """

  import DBN.Binary

  @version_1 1
  @version_2 2

  @v1_symbol_cstr_len 22

  @max_u8 0xFF
  @max_u16 0xFFFF
  @max_u64 0xFFFFFFFFFFFFFFFF

  @stypes %{
    # Symbology using a unique numeric ID.
    0 => :instrument_id,
    # Symbology using the original symbols provided by the publisher.
    1 => :raw_symbol,
    # A set of Databento-specific symbologies for referring to groups of symbols.
    # [deprecated(since = "0.5.0", note = "Smart was split into Continuous and Parent.")]
    2 => :smart,
    # A Databento-specific symbology where one symbol may point to different
    # instruments at different points of time, e.g. to always refer to the front month
    # future.
    3 => :continuous,
    # A Databento-specific symbology for referring to a group of symbols by one
    # "parent" symbol, e.g. ES.FUT to refer to all ES futures.
    4 => :parent,
    # Symbology for US equities using NASDAQ Integrated suffix conventions.
    5 => :nasdaq_symbol,
    # Symbology for US equities using CMS suffix conventions.
    6 => :cms_symbol,
    # Symbology using International Security Identification Numbers (ISIN) - ISO 6166.
    7 => :isin,
    # Symbology using US domestic Committee on Uniform Securities Identification Procedure (CUSIP) codes.
    8 => :us_code,
    # Symbology using Bloomberg composite global IDs.
    9 => :bbg_comp_id,
    # Symbology using Bloomberg composite tickers.
    10 => :bbg_comp_ticker,
    # Symbology using Bloomberg FIGI exchange level IDs.
    11 => :figi,
    # Symbology using Bloomberg exchange level tickers.
    12 => :figi_ticker
  }

  @schemas %{
    # Market by order.
    0 => :mbo,
    # Market by price with a book depth of 1.
    1 => :mbp_1,
    # Market by price with a book depth of 10.
    2 => :mbp_10,
    # All trade events with the best bid and offer (BBO) immediately **before** the
    # effect of the trade.
    3 => :tbbo,
    # All trade events.
    4 => :trades,
    # Open, high, low, close, and volume at a one-second interval.
    5 => :ohlcv_1s,
    # Open, high, low, close, and volume at a one-minute interval.
    6 => :ohlcv_1m,
    # Open, high, low, close, and volume at an hourly interval.
    7 => :ohlcv_1h,
    # Open, high, low, close, and volume at a daily interval based on the UTC date.
    8 => :ohlcv_1d,
    # Instrument definitions.
    9 => :definition,
    # Additional data disseminated by publishers.
    10 => :statistics,
    # Trading status events.
    11 => :status,
    # Auction imbalance events.
    12 => :imbalance,
    # Open, high, low, close, and volume at a daily cadence based on the end of the
    # trading session.
    13 => :ohlcv_eod,
    # Consolidated best bid and offer.
    14 => :cmbp_1,
    # Consolidated best bid and offer subsampled at one-second intervals, in addition
    # to trades.
    15 => :cbbo_1s,
    # Consolidated best bid and offer subsampled at one-minute intervals, in addition
    # to trades.
    16 => :cbbo_1m,
    # All trade events with the consolidated best bid and offer (CBBO) immediately
    # **before** the effect of the trade.
    17 => :tcbbo,
    # Best bid and offer subsampled at one-second intervals, in addition to trades.
    18 => :bbo_1s,
    # Best bid and offer subsampled at one-minute intervals, in addition to trades.
    19 => :bbo_1m
  }

  @doc """
  Decode DBN encoded binary.
  """
  def decode(<<"DBN", version::uint(8), length::uint(32), meta::bytes(length), rest::binary>>) do
    IO.puts("Records: #{byte_size(rest)} bytes")
    _metadata = decode_metadata(version, meta)
  end

  # ----------------------------------------------------------------------------
  # Field decoding
  # ----------------------------------------------------------------------------

  # Decode the Version 1 metadata.
  defp decode_metadata(@version_1, metadata) do
    <<
      dataset::bytes(16),
      schema::uint(16),
      t_start::uint(64),
      t_end::uint(64),
      limit::uint(64),
      _reserved_4::bytes(4),
      stype_in::uint(8),
      stype_out::uint(8),
      ts_out::uint(8),
      _reserved_47::bytes(47),
      schema_definition_length::uint(32),
      schema_definition::bytes(schema_definition_length),
      symbols_count::uint(32),
      symbols::bytes(symbols_count * @v1_symbol_cstr_len),
      partial_count::uint(32),
      partial::bytes(partial_count * @v1_symbol_cstr_len),
      not_found_count::uint(32),
      not_found::bytes(not_found_count * @v1_symbol_cstr_len),
      # mappings_count::uint(32),
      mappings::binary
    >> = metadata

    %DBN.Metadata{
      version: @version_1,
      dataset: trim_nulls(dataset),
      schema: decode_schema(schema),
      schema_definition: schema_definition,
      start: decode_start(t_start),
      end: decode_end(t_end),
      limit: decode_limit(limit),
      stype_in: decode_stype(stype_in),
      stype_out: decode_stype(stype_out),
      ts_out: ts_out == 255,
      symbols: decode_symbols(symbols, @v1_symbol_cstr_len),
      partial: decode_symbols(partial, @v1_symbol_cstr_len),
      not_found: decode_symbols(not_found, @v1_symbol_cstr_len),
      mappings: decode_symbol_mappings(mappings, @v1_symbol_cstr_len)
    }
  end

  defp decode_metadata(@version_2, metadata) do
    <<
      dataset::bytes(16),
      schema::uint(16),
      t_start::uint(64),
      t_end::uint(64),
      limit::uint(64),
      stype_in::uint(8),
      stype_out::uint(8),
      ts_out::uint(8),
      symbol_cstr_len::uint(16),
      _reserved_53::bytes(53),
      schema_definition_length::uint(32),
      schema_definition::bytes(schema_definition_length),
      symbols_count::uint(32),
      symbols::bytes(symbols_count * symbol_cstr_len),
      partial_count::uint(32),
      partial::bytes(partial_count * symbol_cstr_len),
      not_found_count::uint(32),
      not_found::bytes(not_found_count * symbol_cstr_len),
      _mappings_count::uint(32),
      mappings::binary
    >> = metadata

    %DBN.Metadata{
      version: @version_2,
      dataset: trim_nulls(dataset),
      schema: decode_schema(schema),
      schema_definition: schema_definition,
      start: decode_start(t_start),
      end: decode_end(t_end),
      limit: decode_limit(limit),
      stype_in: decode_stype(stype_in),
      stype_out: decode_stype(stype_out),
      ts_out: ts_out == 255,
      symbols: decode_symbols(symbols, symbol_cstr_len),
      partial: decode_symbols(partial, symbol_cstr_len),
      not_found: decode_symbols(not_found, symbol_cstr_len),
      mappings: decode_symbol_mappings(mappings, symbol_cstr_len)
    }
  end

  # `u16::MAX` indicates a potential mix of schemas and record types, which
  # will always be the case for live data.
  defp decode_schema(@max_u16), do: nil

  for {i, schema} <- @schemas do
    defp decode_schema(unquote(i)), do: unquote(schema)
  end

  # `0` indicates no limit.
  defp decode_limit(0), do: :infinity
  defp decode_limit(limit), do: limit

  defp decode_start(t_start), do: DateTime.from_unix!(t_start, :nanosecond)

  # `u64::MAX` indicates no end time was provided.
  defp decode_end(@max_u64), do: nil
  defp decode_end(t_start), do: DateTime.from_unix!(t_start, :nanosecond)

  # `u8::MAX` indicates a potential mix of types, such as with live data.
  defp decode_stype(@max_u8), do: nil

  for {i, type} <- @stypes do
    defp decode_stype(unquote(i)), do: unquote(type)
  end

  # Parse the symbols according to the `symbol_cstr_len`.
  defp decode_symbols(symbols, symbol_cstr_len, acc \\ [])
  defp decode_symbols(<<>>, _symbol_cstr_len, acc), do: Enum.reverse(acc)

  defp decode_symbols(symbols, symbol_cstr_len, acc) do
    <<symbol::bytes(symbol_cstr_len), rest::binary>> = symbols
    decode_symbols(rest, symbol_cstr_len, [trim_nulls(symbol) | acc])
  end

  # Turns an integer date into a `Date.t`.
  defp decode_date(date) when is_integer(date) do
    <<y1::8, y2::8, y3::8, y4::8, m1::8, m2::8, d1::8, d2::8>> = Integer.to_string(date)

    # KEKW
    # Trying to find the way to do this with the least performance hit. ¯\_(ツ)_/¯
    # Converting it to a string already sucks, so trying to not go and
    # use `String.to_integer/1` to convert each date segment back.
    Date.new!(
      (y1 - ?0) * 1000 + (y2 - ?0) * 100 + (y3 - ?0) * 10 + y4 - ?0,
      (m1 - ?0) * 10 + m2 - ?0,
      (d1 - ?0) * 10 + d2 - ?0
    )
  end

  # Decode mappings according to `SymbolMapping`.
  # See the docs for more info:
  #  - https://databento.com/docs/standards-and-conventions/databento-binary-encoding#metadata
  defp decode_symbol_mappings(mappings, symbol_cstr_len, acc \\ [])
  defp decode_symbol_mappings(<<>>, _, acc), do: Enum.reverse(acc)

  defp decode_symbol_mappings(mappings, symbol_cstr_len, acc) do
    <<
      raw_symbol::bytes(symbol_cstr_len),
      interval_count::uint(32),
      intervals::binary
    >> = mappings

    {intervals, rest} = decode_mapping_intervals(intervals, interval_count, symbol_cstr_len)

    mapping = %{
      raw_symbol: trim_nulls(raw_symbol),
      intervals: intervals
    }

    decode_symbol_mappings(rest, symbol_cstr_len, [mapping | acc])
  end

  # Decode mapping intervals according to `MappingInterval`.
  defp decode_mapping_intervals(intervals, count, symbol_cstr_len, acc \\ [])
  defp decode_mapping_intervals(rest, 0, _, acc), do: {Enum.reverse(acc), rest}

  defp decode_mapping_intervals(intervals, count, symbol_cstr_len, acc) do
    <<
      start_date::uint(32),
      end_date::uint(32),
      symbol::bytes(symbol_cstr_len),
      rest::binary
    >> = intervals

    interval = %{
      start_date: decode_date(start_date),
      end_date: decode_date(end_date),
      symbol: trim_nulls(symbol)
    }

    decode_mapping_intervals(rest, count - 1, symbol_cstr_len, [interval | acc])
  end
end
