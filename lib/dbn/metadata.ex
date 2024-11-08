defmodule DBN.Metadata do
  @moduledoc """
  DBN metadata struct.
  """

  @enforce_keys [:version]
  defstruct [
    :version,
    :dataset,
    :schema,
    :start,
    :end,
    :limit,
    :stype_in,
    :stype_out,
    :ts_out,
    :symbols,
    :partial,
    :not_found,
    :mappings
  ]
end
