package tr.unvercanunlu.compare_match.dao;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import tr.unvercanunlu.compare_match.entity.Sample;

import java.util.UUID;

@Repository
public interface ISampleRepository extends JpaRepository<Sample, UUID> {
}