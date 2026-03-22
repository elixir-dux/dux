defmodule Dux.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      %{id: :pg, start: {:pg, :start_link, []}},
      {Dux.Connection, []},
      {DynamicSupervisor, name: Dux.DynamicSupervisor, strategy: :one_for_one}
    ]

    opts = [strategy: :one_for_one, name: Dux.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
