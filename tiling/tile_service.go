package tiling

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"github.com/rs/zerolog/log"
	"github.com/uber/h3-go/v4"
	"math"
	"text/template"
)

type TileService struct {
	db *sql.DB
}

func NewService(db *sql.DB) *TileService {
	return &TileService{
		db: db,
	}
}

type TileQuery struct {
	X        uint32
	Y        uint32
	Z        uint32
	Query    string
	GeoCol   string
	Srid     string
	Compress bool
}

func (tileService *TileService) GetTile(
	ctx context.Context,
	tileQuery *TileQuery,
) ([]byte, error) {
	h3Resolution := translateZoomToH3Resolution(tileQuery.Z)

	// Template for high zoom levels (z >= 15) - no H3 clustering
	rawQueryTemplate := `
		{{define "query"}}
			WITH 
			bounds AS (
				SELECT ST_TileEnvelope({{.z}}, {{.x}}, {{.y}}) AS geom
			),
			mvtgeom AS (
				SELECT
					ST_AsMVTGeom(
						CASE 
							WHEN ST_GeometryType({{.geoCol}}) = 'ST_Point' THEN {{.geoCol}}
							WHEN ST_GeometryType({{.geoCol}}) = 'ST_GeometryCollection' THEN ST_CollectionExtract(ST_Simplify({{.geoCol}}, 0.7 / (2 ^ {{.z}}), true))
							ELSE ST_Simplify({{.geoCol}}, 0.7 / (2 ^ {{.z}}), true)
						END,
						bounds.geom,
						4096,
						256,
						true
					) AS geom,
					t.*,
					1 AS h3clustercount
				FROM ({{.query}}) t, bounds
				WHERE ST_Intersects({{.geoCol}}, bounds.geom)
			)
			SELECT ST_AsMVT(mvtgeom, 'default', 4096, 'geom') AS mvt
			FROM mvtgeom
			WHERE geom IS NOT NULL
		{{end}}
	`

	// Template for lower zoom levels (z < 15) - with H3 clustering for points
	if h3Resolution < h3.MaxResolution {
		rawQueryTemplate = `
		{{define "query"}}
			WITH 
			bounds AS (
				SELECT ST_TileEnvelope({{.z}}, {{.x}}, {{.y}}) AS geom
			),
			source_data AS (
				SELECT 
					t.*,
					ST_GeometryType({{.geoCol}}) AS __geom_type__
				FROM ({{.query}}) t, bounds
				WHERE ST_Intersects({{.geoCol}}, bounds.geom)
			),
			shapes AS (
				SELECT 
					{{.geoCol}},
					t.*,
					1 AS h3clustercount
				FROM source_data t
				WHERE __geom_type__ <> 'ST_Point'
			),
			points_indexed AS (
				SELECT 
					h3_lat_lng_to_cell(CAST({{.geoCol}} AS point), {{.h3Resolution}}) AS __h3_index__,
					t.*
				FROM source_data t
				WHERE __geom_type__ = 'ST_Point'
			),
			points_clustered AS (
				SELECT DISTINCT ON (__h3_index__)
					{{.geoCol}},
					pi.*,
					COUNT(*) OVER (PARTITION BY __h3_index__) AS h3clustercount
				FROM points_indexed pi
				ORDER BY __h3_index__
			),
			combined AS (
				SELECT {{.geoCol}}, h3clustercount, s.* FROM shapes s
				UNION ALL
				SELECT {{.geoCol}}, h3clustercount, pc.* FROM points_clustered pc
			),
			mvtgeom AS (
				SELECT
					ST_AsMVTGeom(
						CASE 
							WHEN ST_GeometryType({{.geoCol}}) = 'ST_Point' THEN {{.geoCol}}
							WHEN ST_GeometryType({{.geoCol}}) = 'ST_GeometryCollection' THEN ST_CollectionExtract(ST_Simplify({{.geoCol}}, 0.7 / (2 ^ {{.z}}), true))
							ELSE ST_Simplify({{.geoCol}}, 0.7 / (2 ^ {{.z}}), true)
						END,
						bounds.geom,
						4096,
						256,
						true
					) AS geom,
					c.*
				FROM combined c, bounds
			)
			SELECT ST_AsMVT(mvtgeom, 'default', 4096, 'geom') AS mvt
			FROM mvtgeom
			WHERE geom IS NOT NULL
		{{end}}
		`
	}

	queryTemplate := template.Must(template.New("").Parse(rawQueryTemplate))
	queryBuffer := &bytes.Buffer{}
	err := queryTemplate.ExecuteTemplate(queryBuffer, "query", map[string]interface{}{
		"geoCol":       tileQuery.GeoCol,
		"query":        tileQuery.Query,
		"h3Resolution": h3Resolution,
		"x":            tileQuery.X,
		"y":            tileQuery.Y,
		"z":            tileQuery.Z,
	})
	if err != nil {
		return nil, err
	}

	sqlQuery := queryBuffer.String()
	log.Debug().Msg(sqlQuery)

	var mvtData []byte
	err = tileService.db.QueryRowContext(ctx, sqlQuery).Scan(&mvtData)
	if err != nil {
		return nil, err
	}

	if tileQuery.Compress {
		return gzipCompress(mvtData)
	}

	return mvtData, nil
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func translateZoomToH3Resolution(z uint32) int {
	if z >= 15 {
		return h3.MaxResolution
	}

	resolution := math.Floor(((1.8 / 3.0) * float64(z)) + 2)
	return int(math.Min(float64(15), resolution))
}
