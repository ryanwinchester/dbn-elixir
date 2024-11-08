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

  @version_1 1
  @version_2 2

  @v1_symbol_cstr_len 22

  @max_u8 0xFF
  @max_u16 0xFFFF
  @max_u64 0xFFFFFFFFFFFFFFFF

  import DBN.Binary

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
      mappings_length::uint(32),
      mappings::bytes(mappings_length),
      rest::binary
    >> = metadata

    IO.inspect(rest, label: "V1 rest #{byte_size(rest)} bytes")

    %DBN.Metadata{
      version: @version_1,
      dataset: trim_nulls(dataset),
      schema: decode_schema(schema),
      schema_definition: schema_definition,
      start: decode_start(t_start),
      end: decode_end(t_end),
      limit: decode_limit(limit),
      stype_in: decode_stype_in(stype_in),
      stype_out: decode_stype_out(stype_out),
      ts_out: ts_out,
      symbols: decode_symbols(symbols, @v1_symbol_cstr_len),
      partial: decode_symbols(partial, @v1_symbol_cstr_len),
      not_found: decode_symbols(not_found, @v1_symbol_cstr_len),
      mappings: decode_mappings(mappings, @v1_symbol_cstr_len)
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
      mappings_length::uint(32),
      mappings::bytes(mappings_length),
      rest::binary
    >> = metadata

    IO.inspect(rest, label: "V2 rest #{byte_size(rest)} bytes")

    %DBN.Metadata{
      version: @version_2,
      dataset: trim_nulls(dataset),
      schema: decode_schema(schema),
      schema_definition: schema_definition,
      start: decode_start(t_start),
      end: decode_end(t_end),
      limit: decode_limit(limit),
      stype_in: decode_stype_in(stype_in),
      stype_out: decode_stype_out(stype_out),
      ts_out: ts_out,
      symbols: decode_symbols(symbols, symbol_cstr_len),
      partial: decode_symbols(partial, symbol_cstr_len),
      not_found: decode_symbols(not_found, symbol_cstr_len),
      mappings: decode_mappings(mappings, symbol_cstr_len)
    }
  end

  # `u16::MAX` indicates a potential mix of schemas and record types, which
  # will always be the case for live data.
  defp decode_schema(@max_u16), do: nil
  defp decode_schema(schema), do: schema

  # `0` indicates no limit.
  defp decode_limit(0), do: :infinity
  defp decode_limit(limit), do: limit

  defp decode_start(t_start), do: DateTime.from_unix!(t_start, :nanosecond)

  # `u64::MAX` indicates no end time was provided.
  defp decode_end(@max_u64), do: nil
  defp decode_end(t_start), do: DateTime.from_unix!(t_start, :nanosecond)

  # `u8::MAX` indicates a potential mix of types, such as with live data.
  defp decode_stype_in(@max_u8), do: nil
  defp decode_stype_in(stype_in), do: stype_in

  # `u8::MAX` indicates a potential mix of types, such as with live data.
  defp decode_stype_out(@max_u8), do: nil
  defp decode_stype_out(stype_out), do: stype_out

  # Parse the symbols according to the `symbol_cstr_len`.
  defp decode_symbols(symbols, symbol_cstr_len, acc \\ [])
  defp decode_symbols(<<>>, _symbol_cstr_len, acc), do: Enum.reverse(acc)

  defp decode_symbols(symbols, symbol_cstr_len, acc) do
    <<symbol::bytes(symbol_cstr_len), rest::binary>> = symbols
    decode_symbols(rest, symbol_cstr_len, [trim_nulls(symbol) | acc])
  end

  # Decode mappings according to `SymbolMapping`.
  # See the docs for more info:
  #  - https://databento.com/docs/standards-and-conventions/databento-binary-encoding#metadata
  defp decode_mappings(mappings, symbol_cstr_len, acc \\ [])
  defp decode_mappings(<<>>, _, acc), do: Enum.reverse(acc)

  defp decode_mappings(mappings, symbol_cstr_len, acc) do
    <<
      raw_symbol::bytes(symbol_cstr_len),
      interval_length::uint(32),
      intervals::bytes(interval_length),
      rest::binary
    >> = mappings

    mapping = %{
      raw_symbol: trim_nulls(raw_symbol),
      intervals: decode_mapping_intervals(intervals, symbol_cstr_len)
    }

    decode_mappings(rest, symbol_cstr_len, [mapping | acc])
  end

  # Decode mapping intervals according to `MappingInterval`.
  defp decode_mapping_intervals(intervals, symbol_cstr_len, acc \\ [])
  defp decode_mapping_intervals(<<>>, _, acc), do: Enum.reverse(acc)

  defp decode_mapping_intervals(intervals, symbol_cstr_len, acc) do
    <<
      start_date::uint(32),
      end_date::uint(32),
      symbol::bytes(symbol_cstr_len),
      rest::binary
    >> = intervals

    interval = %{
      start_date: start_date,
      end_date: end_date,
      symbol: trim_nulls(symbol)
    }

    decode_mapping_intervals(rest, symbol_cstr_len, [interval | acc])
  end
end
