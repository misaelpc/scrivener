defmodule Scrivener do
  @moduledoc """
  Scrivener allows you to paginate your Ecto queries. It gives you useful information such as the total number of pages, the current page, and the current page's entries. It works nicely with Phoenix as well.

  First, you'll want to `use` Scrivener in your application's Repo. This will add a `paginate` function to your Repo. This `paginate` function expects to be called with, at a minimum, an Ecto query. It will then paginate the query and execute it, returning a `Scrivener.Page`. Defaults for `page_size` can be configued when you `use` Scrivener. If no `page_size` is provided, Scrivener will use `10` by default.

  You may also want to call `paginate` with a params map along with your query. If provided with a params map, Scrivener will use the values in the keys `"page"` and `"page_size"` before using any configured defaults.

      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app
        use Scrivener, page_size: 10, max_page_size: 100
      end

      defmodule MyApp.Person do
        use Ecto.Model

        schema "people" do
          field :name, :string
          field :age, :integer

          has_many :friends, MyApp.Person
        end
      end

      def index(conn, params) do
        page = MyApp.Person
        |> where([p], p.age > 30)
        |> order_by([p], desc: p.age)
        |> preload(:friends)
        |> MyApp.Repo.paginate(params)

        render conn, :index,
          people: page.entries,
          page_number: page.page_number,
          page_size: page.page_size,
          total_pages: page.total_pages,
          total_entries: page.total_entries
      end

      page = MyApp.Person
      |> where([p], p.age > 30)
      |> order_by([p], desc: p.age)
      |> preload(:friends)
      |> MyApp.Repo.paginate(page: 2, page_size: 5)
  """

  import Ecto.Query

  alias Scrivener.Config
  alias Scrivener.Page

  @doc """
  Scrivener is meant to be `use`d by an Ecto repository.

  When `use`d, an optional default for `page_size` can be provided. If `page_size` is not provided a default of 10 will be used.

  A `max_page_size` can also optionally can be provided. This enforces a hard ceiling for the page size, even if you allow users of your application to specify `page_size` via query parameters. If not provided, there will be no limit to page size.

      defmodule MyApp.Repo do
        use Ecto.Repo, ...
        use Scrivener
      end

      defmodule MyApp.Repo do
        use Ecto.Repo, ...
        use Scrivener, page_size: 5, max_page_size: 100
      end

    When `use` is called, a `paginate` function is defined in the Ecto repo. See the `paginate` documentation for more information.
  """
  defmacro __using__(opts) do
    quote do
      @scrivener_defaults unquote(opts)

      @spec paginate(Ecto.Query.t, map | Keyword.t) :: Scrivener.Page.t
      def paginate(query, options \\ []) do
        Scrivener.paginate(__MODULE__, @scrivener_defaults, query, options)
      end
    end
  end

  @doc """
  The `paginate` function can also be called with a `Scrivener.Config` for more fine-grained configuration. In this case, it is called directly on the `Scrivener` module.

      config = %Scrivener.Config{
        page_size: 5,
        page_number: 2,
        repo: MyApp.Repo
      }

      MyApp.Model
      |> where([m], m.field == "value")
      |> Scrivener.paginate(config)
  """
  @spec paginate(Ecto.Query.t, Scrivener.Config.t) :: Scrivener.Page.t
  def paginate(query, %Config{page_size: page_size, page_number: page_number, repo: repo}) do
    query = Ecto.Queryable.to_query(query)
    total_entries = total_entries(query, repo)

    %Page{
      page_size: page_size,
      page_number: page_number,
      entries: entries(query, repo, page_number, page_size),
      total_entries: total_entries,
      total_pages: total_pages(total_entries, page_size)
    }
  end

  @doc """
  This method is not meant to be called directly, but rather will be delegated to by calling `paginate/2` on the repository that `use`s Scrivener.

      defmodule MyApp.Repo do
        use Ecto.Repo, ...
        use Scrivener
      end

      MyApp.Model |> where([m], m.field == "value") |> MyApp.Repo.paginate

  When calling your repo's `paginate` function, you may optionally specify `page` and `page_size`. These values can be specified either as a Keyword or map. The values should be integers or string representations of integers.

      MyApp.Model |> where([m], m.field == "value") |> MyApp.Repo.paginate(page: 2, page_size: 10)

      MyApp.Model |> where([m], m.field == "value") |> MyApp.Repo.paginate(%{"page" => "2", "page_size" => "10"})

  The ability to call paginate with a map with string key/values is convenient because you can pass your Phoenix params map to paginate.
  """
  @spec paginate(Ecto.Repo.t, Keyword.t, Ecto.Query.t, map | Keyword.t) :: Scrivener.Page.t
  def paginate(repo, defaults, query, opts) do
    paginate(query, Config.new(repo, defaults, opts))
  end

  defp ceiling(float) do
    t = trunc(float)

    case float - t do
      neg when neg < 0 ->
        t
      pos when pos > 0 ->
        t + 1
      _ -> t
    end
  end

  defp entries(query, repo, page_number, page_size) do
    offset = page_size * (page_number - 1)

    if joins?(query) do
      ids = query
      |> remove_clauses
      |> select([x], {x.id})
      |> group_by([x], x.id)
      |> offset([_], ^offset)
      |> limit([_], ^page_size)
      |> repo.all
      |> Enum.map(&elem(&1, 0))

      query
      |> where([x], x.id in ^ids)
      |> distinct(true)
      |> repo.all
    else
      query = 
        query
        |> limit([_], ^page_size)
        |> offset([_], ^offset)

      {_, query_params} = Ecto.Adapters.SQL.to_sql(:all, repo, query)

      rfc_emitter = Enum.at(query_params, 0)
      rfc_receiver = Enum.at(query_params, 1)
      serie = Enum.at(query_params, 2)
      folio = Enum.at(query_params, 3)
      fecha_inicio = Enum.at(query_params, 4)
      fecha_fin = Enum.at(query_params, 5)
      tipo_comprobante = Enum.at(query_params, 6)
      monto = Enum.at(query_params, 7)

      up_limit = offset + page_size

      case repo == Bemus.Repo do
        true ->

          query_str = "WITH \"hades_results\" AS (SELECT comprobantes.document_id, comprobantes.client_id, comprobantes.receipt_serie, comprobantes.receipt_folio, comprobantes.rfc_emitter, comprobantes.rfc_receiver, comprobantes.status, comprobantes.issue_date, comprobantes.receipt_type, comprobantes.total, cfdis.uuid, ROW_NUMBER() OVER (ORDER BY comprobantes.\"issue_date\" DESC) AS rowNum from \"hades_cfdi_3_2_comprobantes\" AS comprobantes INNER JOIN \"hades_sealed_cfdis\" cfdis ON cfdis.id = comprobantes.document_id"

          #filters
          query_str =
            case rfc_emitter do
              "" ->
                query_str
              value ->
                query_str <> " WHERE comprobantes.rfc_emitter = '#{rfc_emitter}' "
            end

          query_str = 
            case rfc_receiver do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.rfc_receiver = '#{rfc_receiver}' "
                  false ->
                    query_str <> " WHERE comprobantes.rfc_receiver = '#{rfc_receiver}' "
                end
            end

          query_str = 
            case serie do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.receipt_serie = '#{serie}' "
                  false ->
                    query_str <> " WHERE comprobantes.receipt_serie = '#{serie}' "
                end
            end

          query_str = 
            case folio do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.receipt_folio = '#{folio}' "
                  false ->
                    query_str <> " WHERE comprobantes.receipt_folio = '#{folio}' "
                end
            end

          query_str =
            case fecha_inicio do
              {{1, 1, 1}, {0, 0, 0, 0}} ->
                query_str
              value ->
                {{yyyy, mm, dd}, _} = value
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd}' "
                  false ->
                    query_str <> " WHERE comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd}' "
                end
            end

          query_str =
            case fecha_fin do
              {{1, 1, 1}, {0, 0, 0, 0}} ->
                query_str
              value ->
                {{yyyy, mm, dd}, _} = value
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.issue_date <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                  false ->
                    query_str <> " WHERE comprobantes.issue_date <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                end
            end

          query_str =
            case limit_date do
              {{1, 1, 1}, {0, 0, 0, 0}} ->
                query_str
              value ->
                {{yyyy, mm, dd}, _} = value
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                  false ->
                    query_str <> " WHERE comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                end
            end
          
          query_str = 
            case tipo_comprobante do
              "" ->
                query_str
              "todos" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.receipt_type = '#{tipo_comprobante}' "
                  false ->
                    query_str <> " WHERE comprobantes.receipt_type = '#{tipo_comprobante}' "
                end
            end

          query_str =
            case monto do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND comprobantes.total = '#{monto}' "
                  false ->
                    query_str <> " WHERE comprobantes.total = '#{monto}' "
                end
            end

          
          query_str = query_str <> ") SELECT * FROM \"hades_results\" WHERE rowNum > #{offset} and RowNum <= #{up_limit}"

          Ecto.Adapters.SQL.query(repo, query_str, [])
        false ->

          query_str = "WITH results AS(SELECT cfdis.idInternal AS document_id, cfdis.Empresa_Id AS client_id, cfdis.serie AS receipt_serie, cfdis.folio AS receipt_folio, e.rfc AS rfc_emitter, cfdis.rfc AS rfc_receiver, cfdis.vigente AS status, cfdis.fechaGeneracion AS issue_date, cfdis.tipoDeComprobante AS receipt_type, cfdis.montoTotal AS total, cfdis.idInternal AS uuid, ROW_NUMBER() OVER (ORDER BY cfdis.fechaGeneracion DESC) AS rowNum from CFD AS cfdis INNER JOIN EMPRESA e ON e.idInternal = cfdis.Empresa_Id"

          #filters
          query_str =
            case rfc_emitter do
              "" ->
                query_str
              value ->
                query_str <> " WHERE e.rfc = '#{rfc_emitter}' "
            end

          query_str = 
            case rfc_receiver do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.rfc = '#{rfc_receiver}' "
                  false ->
                    query_str <> " WHERE cfdis.rfc = '#{rfc_receiver}' "
                end
            end

          query_str = 
            case serie do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.serie = '#{serie}' "
                  false ->
                    query_str <> " WHERE cfdis.serie = '#{serie}' "
                end
            end

          query_str = 
            case folio do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.folio = '#{folio}' "
                  false ->
                    query_str <> " WHERE cfdis.folio = '#{folio}' "
                end
            end

          query_str =
            case fecha_inicio do
              {{1, 1, 1}, {0, 0, 0, 0}} ->
                query_str
              value ->
                {{yyyy, mm, dd}, _} = value
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd}' "
                  false ->
                    query_str <> " WHERE cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd}' "
                end
            end

          query_str =
            case fecha_fin do
              {{1, 1, 1}, {0, 0, 0, 0}} ->
                query_str
              value ->
                {{yyyy, mm, dd}, _} = value
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.fechaGeneracion <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                  false ->
                    query_str <> " WHERE cfdis.fechaGeneracion <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                end
            end

          query_str =
            case limit_date do
              {{1, 1, 1}, {0, 0, 0, 0}} ->
                query_str
              value ->
                {{yyyy, mm, dd}, _} = value
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd}' "
                  false ->
                    query_str <> " WHERE cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd}' "
                end
            end
          
          query_str = 
            case tipo_comprobante do
              "" ->
                query_str
              "todos" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.tipoDeComprobante = '#{tipo_comprobante}' "
                  false ->
                    query_str <> " WHERE cfdis.tipoDeComprobante = '#{tipo_comprobante}' "
                end
            end

          query_str =
            case monto do
              "" ->
                query_str
              value ->
                case String.contains?(query_str, "WHERE") do
                  true ->
                    query_str <> " AND cfdis.montoTotal = '#{monto}' "
                  false ->
                    query_str <> " WHERE cfdis.montoTotal = '#{monto}' "
                end
            end

          query_str = query_str <> ") SELECT * FROM \"results\" WHERE rowNum > #{offset} and RowNum <= #{up_limit}"

          Ecto.Adapters.SQL.query(repo, query_str, [])
      end
    end
  end

  defp joins?(query) do
    Enum.count(query.joins) > 0
  end

  defp remove_clauses(query) do
    query
    |> exclude(:preload)
    |> exclude(:select)
    |> exclude(:group_by)
  end

  defp total_entries(query, repo) do
    primary_key = query.from
    |> elem(1)
    |> apply(:__schema__, [:primary_key])
    |> hd

    query =
      query
      |> remove_clauses
      |> exclude(:order_by)
      |> select([m], count(field(m, ^primary_key), :distinct))
      #|> repo.one!
    {_, query_params} = Ecto.Adapters.SQL.to_sql(:all, repo, query)

    rfc_emitter = Enum.at(query_params, 0)
    rfc_receiver = Enum.at(query_params, 1)
    serie = Enum.at(query_params, 2)
    folio = Enum.at(query_params, 3)
    fecha_inicio = Enum.at(query_params, 4)
    fecha_fin = Enum.at(query_params, 5)
    limit_date = Enum.at(query_params, 6)
    tipo_comprobante = Enum.at(query_params, 7)
    monto = Enum.at(query_params, 8)

    case repo == Bemus.Repo do
      true ->

        query_str = "SELECT count(DISTINCT [id]) FROM [hades_sealed_cfdis] AS cfdis INNER JOIN [hades_cfdi_3_2_comprobantes] AS comprobantes ON comprobantes.document_id = cfdis.id"

        #filters
        query_str =
          case rfc_emitter do
            "" ->
              query_str
            value ->
              query_str <> " WHERE comprobantes.rfc_emitter = '#{rfc_emitter}' "
          end

        query_str = 
          case rfc_receiver do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.rfc_receiver = '#{rfc_receiver}' "
                false ->
                  query_str <> " WHERE comprobantes.rfc_receiver = '#{rfc_receiver}' "
              end
          end

        query_str = 
          case serie do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.receipt_serie = '#{serie}' "
                false ->
                  query_str <> " WHERE comprobantes.receipt_serie = '#{serie}' "
              end
          end

        query_str = 
          case folio do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.receipt_folio = '#{folio}' "
                false ->
                  query_str <> " WHERE comprobantes.receipt_folio = '#{folio}' "
              end
          end

        query_str =
          case fecha_inicio do
            {{1, 1, 1}, {0, 0, 0, 0}} ->
              query_str
            value ->
              {{yyyy, mm, dd}, _} = value
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd}' "
                false ->
                  query_str <> " WHERE comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd}' "
              end
          end

        query_str =
          case fecha_fin do
            {{1, 1, 1}, {0, 0, 0, 0}} ->
              query_str
            value ->
              {{yyyy, mm, dd}, _} = value
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.issue_date <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                false ->
                  query_str <> " WHERE comprobantes.issue_date <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
              end
          end

        query_str =
          case limit_date do
            {{1, 1, 1}, {0, 0, 0, 0}} ->
              query_str
            value ->
              {{yyyy, mm, dd}, _} = value
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                false ->
                  query_str <> " WHERE comprobantes.issue_date >= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
              end
          end

        query_str = 
          case tipo_comprobante do
            "" ->
              query_str
            "todos" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.receipt_type = '#{tipo_comprobante}' "
                false ->
                  query_str <> " WHERE comprobantes.receipt_type = '#{tipo_comprobante}' "
              end
          end

        query_str =
          case monto do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND comprobantes.total = '#{monto}' "
                false ->
                  query_str <> " WHERE comprobantes.total = '#{monto}' "
              end
          end

        %{columns: _, command: _, num_rows: 1, rows: [[result]]} = Ecto.Adapters.SQL.query!(repo, query_str, [])

        result
      false -> 
        query_str = "SELECT count(DISTINCT [cfdis].[idInternal]) FROM [CFD] AS cfdis INNER JOIN EMPRESA e ON E.idInternal = CFDIS.Empresa_Id"

        #filters
        query_str =
          case rfc_emitter do
            "" ->
              query_str
            value ->
              query_str <> " WHERE e.rfc = '#{rfc_emitter}' "
          end

        query_str = 
          case rfc_receiver do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.rfc = '#{rfc_receiver}' "
                false ->
                  query_str <> " WHERE cfdis.rfc = '#{rfc_receiver}' "
              end
          end

        query_str = 
          case serie do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.serie = '#{serie}' "
                false ->
                  query_str <> " WHERE cfdis.serie = '#{serie}' "
              end
          end

        query_str = 
          case folio do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.folio = '#{folio}' "
                false ->
                  query_str <> " WHERE cfdis.folio = '#{folio}' "
              end
          end

        query_str =
          case fecha_inicio do
            {{1, 1, 1}, {0, 0, 0, 0}} ->
              query_str
            value ->
              {{yyyy, mm, dd}, _} = value
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd}' "
                false ->
                  query_str <> " WHERE cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd}' "
              end
          end

        query_str =
          case fecha_fin do
            {{1, 1, 1}, {0, 0, 0, 0}} ->
              query_str
            value ->
              {{yyyy, mm, dd}, _} = value
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.fechaGeneracion <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                false ->
                  query_str <> " WHERE cfdis.fechaGeneracion <= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
              end
          end

        query_str =
          case limit_date do
            {{1, 1, 1}, {0, 0, 0, 0}} ->
              query_str
            value ->
              {{yyyy, mm, dd}, _} = value
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
                false ->
                  query_str <> " WHERE cfdis.fechaGeneracion >= '#{yyyy}-#{mm}-#{dd} 23:59:59' "
              end
          end

        query_str = 
          case tipo_comprobante do
            "" ->
              query_str
            "todos" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.tipoDeComprobante = '#{tipo_comprobante}' "
                false ->
                  query_str <> " WHERE cfdis.tipoDeComprobante = '#{tipo_comprobante}' "
              end
          end

        query_str =
          case monto do
            "" ->
              query_str
            value ->
              case String.contains?(query_str, "WHERE") do
                true ->
                  query_str <> " AND cfdis.montoTotal = '#{monto}' "
                false ->
                  query_str <> " WHERE cfdis.montoTotal = '#{monto}' "
              end
          end

        %{columns: _, command: _, num_rows: 1, rows: [[result]]} = Ecto.Adapters.SQL.query!(repo, query_str, [])

        result

    end
  end

  defp total_pages(total_entries, page_size) do
    ceiling(total_entries / page_size)
  end
end
