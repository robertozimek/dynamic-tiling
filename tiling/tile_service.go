package tiling

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/paulmach/orb/encoding/mvt"
	"github.com/paulmach/orb/encoding/wkt"
	"github.com/paulmach/orb/geojson"
	"github.com/paulmach/orb/maptile"
	"github.com/robertozimek/dynamic-tiling/internal/utils"
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
	zoom := maptile.Zoom(tileQuery.Z)

	tile := maptile.New(tileQuery.X, tileQuery.Y, zoom)
	bbox := tile.Bound(0.5)

	boundingBox := fmt.Sprintf(
		"ST_MakeEnvelope(%v, %v, %v, %v, %s)",
		bbox.Min.X(),
		bbox.Min.Y(),
		bbox.Max.X(),
		bbox.Max.Y(),
		tileQuery.Srid,
	)

	log.Debug().Msg(boundingBox)

	h3Resolution := translateZoomToH3Resolution(tileQuery.Z)

	rawQueryTemplate := `
		{{define "query"}}
				WITH geometry_type AS (
					SELECT 
						t.*, 
						ST_GeometryType({{.geoCol}}) as __internal_geometry_type__,
						ROUND(0.7 / (2 ^ {{.zoom}})::numeric, 3) as __internal_geometry_simplify__
					FROM ({{.query}}) t
					WHERE 
						ST_INTERSECTS({{.boundingBox}}, {{.geoCol}})
					)				
				SELECT 
					t.*, 
					CASE 
						WHEN 
							__internal_geometry_type__ = 'ST_GeometryCollection'
						THEN 
							ST_ASTEXT(ST_CollectionExtract(ST_Simplify({{.geoCol}}, 0.7 / (2 ^ {{.zoom}}), true)))
						WHEN 
							__internal_geometry_type__ = 'ST_Point'
						THEN 
							ST_ASTEXT({{.geoCol}})
						ELSE 
							ST_ASTEXT(ST_Simplify({{.geoCol}}, t.__internal_geometry_simplify__, true))
					END as __internal_geometry_text__,
					1 as h3ClusterCount
				FROM geometry_type t
		{{end}}
	`
	if h3Resolution < h3.MaxResolution {
		rawQueryTemplate = `
		{{define "query"}}
			WITH geometry_type AS (
				SELECT 
					t.*, 
					ST_GeometryType({{.geoCol}}) as __internal_geometry_type__,
					ROUND(0.7 / (2 ^ {{.zoom}})::numeric, 3) as __internal_geometry_simplify__
				FROM ({{.query}}) t
				WHERE 
					ST_INTERSECTS({{.boundingBox}}, {{.geoCol}})
			), setup AS (
				SELECT 
					t.*, 
					CASE 
						WHEN 
							__internal_geometry_type__ = 'ST_GeometryCollection'
						THEN 
							ST_ASTEXT(ST_CollectionExtract(ST_Simplify({{.geoCol}}, 0.7 / (2 ^ {{.zoom}}), true)))
						WHEN 
							__internal_geometry_type__ = 'ST_Point'
						THEN 
							ST_ASTEXT({{.geoCol}})
						ELSE 
							ST_ASTEXT(ST_Simplify({{.geoCol}}, t.__internal_geometry_simplify__, true))
					END as __internal_geometry_text__
				FROM geometry_type t
			), shapes AS (
				SELECT 
					CAST('1' as h3index) as __internal_h3_index__, 
					*, 
					1 as h3ClusterCount
				FROM setup 
				WHERE __internal_geometry_type__ <> 'ST_Point' AND __internal_geometry_text__ IS NOT NULL
			), points AS (
				(WITH data AS (
					SELECT  * FROM setup WHERE __internal_geometry_type__ = 'ST_Point'
				), indexed AS (
					SELECT h3_lat_lng_to_cell(__internal_geometry_text__, {{.h3Resolution}}) as __internal_h3_index__, * FROM data 
				), counted_index AS (
					SELECT *, row_number() over (partition by __internal_h3_index__ ORDER BY __internal_h3_index__ DESC) as h3ClusterCount FROM indexed
				)
				SELECT 
					distinct on(ci.__internal_h3_index__) ci.*
				FROM counted_index ci
				ORDER BY ci.__internal_h3_index__, ci.h3ClusterCount DESC)
			) SELECT * FROM shapes UNION ALL SELECT * FROM points
		{{end}}
		`
	}

	queryTemplate := template.Must(template.New("").Parse(rawQueryTemplate))
	queryBuffer := &bytes.Buffer{}
	err := queryTemplate.ExecuteTemplate(queryBuffer, "query", map[string]interface{}{
		"geoCol":       tileQuery.GeoCol,
		"query":        tileQuery.Query,
		"h3Resolution": h3Resolution,
		"boundingBox":  boundingBox,
		"zoom":         zoom,
	})
	if err != nil {
		return nil, err
	}

	sqlQuery := queryBuffer.String()
	log.Debug().Msg(sqlQuery)

	rows, err := tileService.db.QueryContext(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	mapOfRows, err := utils.GetMapSliceFromRows(rows)
	if err != nil {
		return nil, err
	}

	features, err := convertRowsToFeatures(*mapOfRows, tileQuery.GeoCol)

	layerMap := make(map[string]*geojson.FeatureCollection)

	featureCollection := geojson.FeatureCollection{
		Type:     "FeatureCollection",
		Features: features,
	}

	if utils.IsDebug() {
		json, err := featureCollection.MarshalJSON()
		if err != nil {
			return nil, err
		}

		log.Debug().Msg(string(json))
	}

	layerMap["default"] = &featureCollection
	layers := mvt.NewLayers(layerMap)
	layers.ProjectToTile(tile)

	if tileQuery.Compress {
		data, err := mvt.MarshalGzipped(layers)
		return data, err
	}

	data, err := mvt.Marshal(layers)
	return data, err
}

func translateZoomToH3Resolution(z uint32) int {
	if z >= 15 {
		return h3.MaxResolution
	}

	resolution := math.Floor(((1.8 / 3.0) * float64(z)) + 2)
	return int(math.Min(float64(15), resolution))
}

func convertRowsToFeatures(mapOfRows []map[string]interface{}, geoCol string) ([]*geojson.Feature, error) {
	features := []*geojson.Feature{}

	for _, row := range mapOfRows {
		properties := make(map[string]interface{})

		omitKeys := []string{
			"__internal_h3_index__",
			"__internal_geometry_type__",
			"__internal_geometry_text__",
			"__internal_geometry_simplify__",
			geoCol,
		}

		if row["__internal_geometry_type__"] != "ST_Point" {
			omitKeys = append(omitKeys, "h3ClusterCount")
		}

		utils.CopyExcludingKeys(row, properties, omitKeys)

		geometryText := row["__internal_geometry_text__"].(string)
		geometry, err := wkt.Unmarshal(geometryText)

		if err != nil {
			return nil, err
		}

		feature := &geojson.Feature{
			Type:       "Feature",
			Properties: properties,
			Geometry:   geometry,
		}
		features = append(features, feature)
	}

	return features, nil
}
